"""Issue #278: a complete snapshot reconciled as appends and deletes.

``ManifestServer`` follows the Server contract in memory: it pages the stored
manifest, applies incremental syncs (with ``orderAuthoritative``), deletes and
complete replacements, and refuses request bodies over a size limit like the
public ingress. Never send history to a real server.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from ingest_servers import MANIFEST_PATH
from test_issue_278_dsh_snapshot import (
    CHECKPOINT,
    CHECKPOINT_KEY,
    EXTERNAL,
    SESSION,
    capture,
    make_items,
)

from connector.runtime_protocol import (
    RuntimeInstanceHost,
    RuntimeInstanceSpec,
    timeline_content_hash,
)
from connector.runtimes.dsh.bridge.sync import SyncRelay
from connector.server import ingest_reconcile
from connector.server.errors import ConnectorNetworkError
from connector.server.ingest import ConnectorIngestClient, ConnectorIngestRejectedError
from connector.server.runtime_host import ConnectorRuntimeHost

MIB = 1024 * 1024


class ManifestServer:
    def __init__(self, limit: int = 50 * MIB, page: int = ingest_reconcile.MANIFEST_PAGE_ITEMS):
        self.limit = limit
        self.page = page
        self.timelines: dict[str, dict[str, dict[str, Any]]] = {}
        self.notes: list[dict[str, Any]] = []
        self.sizes: list[int] = []
        self.statuses: list[int] = []
        self.manifest_reads = 0
        self.manifest_status: int | None = None
        self.offline_after_syncs: int | None = None
        self.refuse_syncs = 0
        self.syncs = 0
        # False: an older Server behind the same address takes the writes.
        self.features = True
        self.manifest_bodies: list[dict[str, Any]] = []

    def ids(self, session: str = SESSION) -> list[str]:
        items = self.timelines.get(session, {}).values()
        return [item["id"] for item in sorted(items, key=lambda item: (item["orderSeq"], item["id"]))]

    def methods(self) -> list[str]:
        return [note["method"] for note in self.notes]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.sizes.append(len(request.content))
        if len(request.content) > self.limit:
            self.statuses.append(413)
            return httpx.Response(413)
        body = json.loads(request.content)
        if request.url.path.endswith(MANIFEST_PATH):
            return self.manifest(body)
        notes = body["notifications"]
        if (self.offline_after_syncs is not None and self.syncs >= self.offline_after_syncs
                and any(note["method"] == "timeline.sync" for note in notes)):
            self.offline_after_syncs = None
            raise httpx.ConnectError("synthetic offline", request=request)
        if any(note["method"] == "session.source.updated"
               and note["params"].get("observationOrigin") not in {"event", "inventory", "operation"}
               for note in notes):
            # Like the Server, a source fact without a valid origin fails the request.
            self.statuses.append(400)
            return httpx.Response(400, json={"detail": {"code": "invalid_session_source_origin"}})
        rejected = []
        for index, note in enumerate(notes):
            params = note["params"]
            if note["method"] == "timeline.sync" and params.get("items") and self.refuse_syncs:
                self.refuse_syncs -= 1
                rejected.append({"index": index, "method": note["method"], "code": "notification_failed",
                                 "message": "refused"})
                continue
            self.apply(note["method"], params)
        self.statuses.append(200)
        self.notes.extend(notes)
        return httpx.Response(200, json={"accepted": len(notes) - len(rejected), "rejected": rejected,
                                         **({"features": ["timeline.reconcile"]} if self.features else {})})

    def apply(self, method: str, params: dict[str, Any]) -> None:
        timeline = self.timelines.setdefault(params.get("sessionId"), {})
        authoritative = params.get("orderAuthoritative") is True and self.features
        if method == "timeline.sync":
            self.syncs += 1
            if params.get("complete") is True:
                timeline.clear()
            for item in params["items"]:
                existing = timeline.get(item["id"])
                if existing is not None and not authoritative:
                    item = {**item, "orderSeq": existing["orderSeq"]}
                timeline[item["id"]] = item
        elif method == "timeline.itemUpsert":
            timeline[params["item"]["id"]] = params["item"]
        elif method == "timeline.delete" and self.features:
            for item_id in params["itemIds"]:
                timeline.pop(item_id, None)

    def manifest(self, body: dict[str, Any]) -> httpx.Response:
        self.manifest_reads += 1
        self.manifest_bodies.append(body)
        if self.manifest_status is not None:
            return httpx.Response(self.manifest_status)
        items = sorted(self.timelines.get(body["sessionId"], {}).values(),
                       key=lambda item: (item["orderSeq"], item["id"]))
        after = body.get("after")
        if after:
            items = [item for item in items if (item["orderSeq"], item["id"]) > (after["orderSeq"], after["id"])]
        page = items[:min(body["limit"], self.page)]
        more = len(items) > len(page)
        return httpx.Response(200, json={
            "items": [{"id": item["id"], "orderSeq": item["orderSeq"], "contentHash": item["contentHash"]}
                      for item in page],
            "next": {"orderSeq": page[-1]["orderSeq"], "id": page[-1]["id"]} if more else None,
        })


def _item(name: str, order: int, text: str = "", size: int = 0) -> dict[str, Any]:
    text = text or name
    return {"id": name, "sessionId": SESSION, "type": "message", "role": "assistant", "status": "done",
            "orderSeq": order, "revision": 1, "content": {"text": text + "x" * size},
            "source": {"runtime": "codex", "sessionId": EXTERNAL}, "contentHash": f"h:{name}:{text}:{size}"}


def _snapshot(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"method": "timeline.sync", "params": {
        "sessionId": SESSION, "runtime": "codex", "runtimeId": "rti_test", "complete": True, "items": items}}


async def _token(force):
    return "test-token"


def _client(http, budget: int = 8 * MIB) -> ConnectorIngestClient:
    client = ConnectorIngestClient("https://aa.example.invalid", _token, lambda: http, lambda _: http)
    client._max_body_bytes = budget
    return client


def _run(server: ManifestServer, *batches: list[dict[str, Any]], budget: int = 8 * MIB) -> None:
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            client = _client(http, budget)
            for batch in batches:
                await client.ingest_notifications(batch)
    asyncio.run(run())


def _seed(server: ManifestServer, items: list[dict[str, Any]]) -> None:
    server.timelines[SESSION] = {item["id"]: item for item in items}


def test_large_snapshot_to_an_empty_timeline_is_appended_in_bounded_pages():
    server = ManifestServer()
    items = [_item(f"i{index:02d}", index + 1, size=5 * MIB) for index in range(11)]
    _run(server, [_snapshot(items)])
    assert set(server.statuses) == {200}
    assert max(server.sizes) <= 8 * MIB
    assert server.ids() == [item["id"] for item in items]
    syncs = [note["params"] for note in server.notes if note["method"] == "timeline.sync"]
    assert all(params["complete"] is False and params["orderAuthoritative"] is True for params in syncs)
    assert "timeline.delete" not in server.methods()


def test_only_changed_items_travel_and_removed_ones_are_deleted():
    server = ManifestServer()
    old = [_item(name, index + 1, size=MIB) for index, name in enumerate("abcdef")]
    _seed(server, old)
    new = [old[0], _item("c", 2, size=MIB), _item("b", 3, size=MIB),
           _item("e", 4, text="edited", size=MIB), _item("x", 5, size=MIB)]
    _run(server, [_snapshot(new)])
    assert server.ids() == ["a", "c", "b", "e", "x"]
    assert server.timelines[SESSION]["e"]["content"]["text"].startswith("edited")
    uploaded = [item["id"] for note in server.notes if note["method"] == "timeline.sync"
                for item in note["params"]["items"]]
    assert uploaded == ["c", "b", "e", "x"], "a is unchanged and stays on the Server"
    deleted = [item_id for note in server.notes if note["method"] == "timeline.delete"
               for item_id in note["params"]["itemIds"]]
    assert sorted(deleted) == ["d", "f"]
    assert sum(server.sizes) < 5 * MIB


def test_unchanged_snapshot_sends_an_empty_sync_naming_its_session():
    server = ManifestServer()
    items = [_item(name, index + 1, size=MIB) for index, name in enumerate("abc")]
    _seed(server, items)
    _run(server, [_snapshot(items)])
    assert server.methods() == ["timeline.sync"]
    params = server.notes[0]["params"]
    assert params["items"] == [] and params["externalSessionId"] == EXTERNAL
    assert server.manifest_bodies[0]["externalSessionId"] == EXTERNAL
    assert server.ids() == ["a", "b", "c"]


def test_a_refused_change_is_not_followed_by_its_deletes():
    server = ManifestServer()
    server.refuse_syncs = 1
    _seed(server, [_item("a", 1), _item("b", 2)])
    with pytest.raises(ConnectorIngestRejectedError):
        _run(server, [_snapshot([_item("x", 1)])])
    assert "timeline.delete" not in server.methods()
    assert server.ids() == ["a", "b"], "nothing the snapshot still has was removed, nothing half-applied"


def test_snapshots_in_one_batch_each_read_the_timeline_the_one_before_left():
    server = ManifestServer()
    a, b = _item("a", 1), _item("b", 2)
    _seed(server, [a, b])
    live = {"method": "timeline.itemUpsert", "params": {"sessionId": SESSION, "item": _item("live", 3)}}
    _run(server, [_snapshot([a]), _snapshot([a, b]), live, _snapshot([a, b])])
    assert server.ids() == ["a", "b"], "as if the snapshots replaced the timeline one after another"
    assert server.manifest_reads == 3


def test_writes_an_older_server_took_without_applying_fall_back_to_a_replacement():
    server = ManifestServer()
    server.features = False
    _seed(server, [_item("a", 1), _item("b", 2)])
    _run(server, [_snapshot([_item("b", 1), _item("x", 2)])])
    assert server.ids() == ["b", "x"]
    assert server.notes[-1]["params"]["complete"] is True
    assert "timeline.delete" not in server.methods(), "replaced right after the first unconfirmed write"


def test_a_repeated_id_keeps_its_last_value():
    server = ManifestServer()
    new = _item("a", 1, text="new")
    _seed(server, [new])
    _run(server, [_snapshot([_item("a", 1, text="old"), new])])
    assert server.timelines[SESSION]["a"]["content"]["text"] == "new"


def test_empty_snapshot_deletes_everything():
    server = ManifestServer()
    _seed(server, [_item(name, index + 1) for index, name in enumerate("abc")])
    _run(server, [_snapshot([])])
    assert server.ids() == []


def test_manifest_pages_are_followed_and_deletes_are_chunked(monkeypatch):
    monkeypatch.setattr(ingest_reconcile, "DELETE_CHUNK_ITEMS", 4)
    server = ManifestServer(page=3)
    _seed(server, [_item(f"o{index}", index + 1) for index in range(10)])
    _run(server, [_snapshot([_item("o0", 1)])])
    assert server.manifest_reads == 4
    assert server.manifest_bodies[0]["limit"] == ingest_reconcile.MANIFEST_PAGE_ITEMS == 5000
    assert server.ids() == ["o0"]
    assert [len(note["params"]["itemIds"]) for note in server.notes if note["method"] == "timeline.delete"] == [
        4, 4, 1]


def test_a_server_without_the_manifest_gets_the_snapshot_whole_and_is_not_asked_again():
    server = ManifestServer()
    server.manifest_status = 404
    items = [_item(name, index + 1) for index, name in enumerate("ab")]
    _run(server, [_snapshot(items)], [_snapshot(items)])
    assert server.manifest_reads == 1
    syncs = [note["params"] for note in server.notes if note["method"] == "timeline.sync"]
    assert [params["complete"] for params in syncs] == [True, True]


def test_a_refused_manifest_sends_the_snapshot_whole():
    server = ManifestServer()
    server.manifest_status = 409
    _run(server, [_snapshot([_item("a", 1)])])
    assert server.notes[0]["params"]["complete"] is True


def test_manifest_outage_keeps_the_snapshot_for_a_retry():
    async def run():
        server = ManifestServer()
        server.manifest_status = 503
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            client = _client(http)
            pending = [_snapshot([_item("a", 1)])]
            with pytest.raises(ConnectorNetworkError):
                await client._post_batch(pending)
            assert pending[0]["params"]["complete"] is True
            server.manifest_status = None
            await client._post_batch(pending)
        assert server.ids() == ["a"]
        assert all(note["params"].get("complete") is False for note in server.notes)
    asyncio.run(run())


def test_network_failure_resumes_at_the_first_unaccepted_page_without_reading_again():
    async def run():
        server = ManifestServer()
        server.offline_after_syncs = 2
        items = [_item(f"i{index}", index + 1, size=20 * 1024) for index in range(6)]
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            client = _client(http, budget=48 * 1024)
            pending = [_snapshot(items)]
            with pytest.raises(ConnectorNetworkError):
                await client._post_batch(pending)
            await client._post_batch(pending)
        assert server.manifest_reads == 1
        uploaded = [item["id"] for note in server.notes if note["method"] == "timeline.sync"
                    for item in note["params"]["items"]]
        assert uploaded == [item["id"] for item in items], "accepted pages are not sent again"
        assert server.ids() == [item["id"] for item in items]
    asyncio.run(run())


def _stack(http, budget: int = 8 * MIB, client: ConnectorIngestClient | None = None):
    client = client or _client(http, budget)
    base = ConnectorRuntimeHost("test-connector", AsyncMock(), AsyncMock(),
                                ingest_notifications=client.ingest_notifications,
                                timeline_manifest_reader=client.read_timeline_manifest)
    bound = RuntimeInstanceHost(base, RuntimeInstanceSpec(runtime_id="rti_test", runtime_type="dsh", name="test"),
                                status_reporter=AsyncMock())
    return bound


def _dsh_items(count: int, size: int) -> list[dict[str, Any]]:
    return make_items(count, size)


def _edited(item: dict[str, Any], text: str) -> dict[str, Any]:
    content = {"text": text, "format": "markdown"}
    return {**item, "content": content,
            "contentHash": timeline_content_hash("message", "done", "assistant", content)}


def test_dsh_capture_over_the_ingress_limit_is_uploaded_while_pages_arrive():
    async def run():
        server = ManifestServer()
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            host = _stack(http)
            relay = SyncRelay(Mock(), host)
            relay.durable_checkpoints = True
            identity = {"sessionId": SESSION, "snapshotId": "c"}
            try:
                items = _dsh_items(11, 5 * MIB)
                await relay.operation({"kind": "snapshot.begin", **identity, "throughSeq": 11, "meta": {
                    "externalSessionId": EXTERNAL, "sourceState": {"availability": "available"}}})
                for item in items:
                    await relay.operation({"kind": "snapshot.items", **identity, "items": [item]})
                # Every page is on the Server before the commit.
                assert server.ids() == [item["id"] for item in items]
                before_commit = len(server.sizes)
                await relay.operation({"kind": "snapshot.commit", **identity, "totalItems": 11, "throughSeq": 11})
                assert sum(server.sizes[before_commit:]) < 64 * 1024, "the commit only restates metadata"
                assert set(server.statuses) == {200} and max(server.sizes) <= 8 * MIB
                assert server.methods()[0] == "session.meta.upsert", "the session exists before its items"
                assert server.manifest_bodies[0]["externalSessionId"] == EXTERNAL
                assert all(note["params"]["externalSessionId"] == EXTERNAL for note in server.notes
                           if note["method"] == "timeline.sync"), "every page names the manifest's session"
                assert not relay.quarantined
                await relay.operation({"kind": "checkpoint.save", "externalSessionId": EXTERNAL,
                                       "checkpoint": CHECKPOINT})
                assert await host.sync_state_read(CHECKPOINT_KEY) == CHECKPOINT
            finally:
                await relay.close()
    asyncio.run(run())


def test_dsh_recapture_uploads_changes_and_deletes_what_is_gone():
    async def run():
        server = ManifestServer()
        _seed(server, _dsh_items(5, 1024))
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            relay = SyncRelay(Mock(), _stack(http))
            try:
                edited = _dsh_items(3, 1024)
                edited[1] = _edited(edited[1], "edited")
                await capture(relay, edited)
                assert server.ids() == ["item-0", "item-1", "item-2"]
                uploaded = [item["id"] for note in server.notes if note["method"] == "timeline.sync"
                            for item in note["params"]["items"]]
                assert uploaded == ["item-1"]
                deleted = [item_id for note in server.notes if note["method"] == "timeline.delete"
                           for item_id in note["params"]["itemIds"]]
                assert sorted(deleted) == ["item-3", "item-4"]
                assert all(note["params"]["externalSessionId"] == EXTERNAL for note in server.notes
                           if note["method"] == "timeline.delete")
            finally:
                await relay.close()
    asyncio.run(run())


def test_dsh_refused_page_falls_back_to_one_reconciled_snapshot():
    async def run():
        server = ManifestServer()
        server.refuse_syncs = 1
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            relay = SyncRelay(Mock(), _stack(http))
            try:
                items = _dsh_items(4, 1024)
                await capture(relay, items)
                assert server.ids() == [item["id"] for item in items]
                assert not relay.quarantined
                assert server.manifest_reads == 2, "the whole path reads the manifest again"
            finally:
                await relay.close()
    asyncio.run(run())


def test_dsh_capture_metadata_keeps_a_newer_host_fact_during_quarantine():
    async def run():
        server = ManifestServer()
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            relay = SyncRelay(Mock(), _stack(http))
            try:
                await relay.quarantine(SESSION, EXTERNAL, 10, ConnectorIngestRejectedError("refused"))
                for task in list(relay.retry_tasks):
                    task.cancel()
                # The Host reports the session archived after the refusal.
                archived = {"availability": "archived", "reason": "archived_in_dsh",
                            "observedAt": "2026-10-10T10:00:00Z", "observationOrigin": "event"}
                relay.durable_checkpoints = True
                await relay.operation({"kind": "notifications", "notifications": [{
                    "method": "session.source.updated", "params": {"sessionId": SESSION, **archived}}]})
                server.notes.clear()
                await relay.operation({"kind": "snapshot.begin", "sessionId": SESSION, "snapshotId": "c",
                                       "throughSeq": 11, "meta": {"externalSessionId": EXTERNAL,
                                       "sourceState": {"availability": "available",
                                                       "observedAt": "2026-10-10T09:00:00Z"}}})
                meta = next(note for note in server.notes if note["method"] == "session.meta.upsert")
                # The Host's own fact, as observed: the backend keeps it.
                assert meta["params"]["sourceState"] == archived
                assert relay.quarantined[SESSION].host_fact is True, "an older capture fact does not reset it"
            finally:
                await relay.close()
    asyncio.run(run())


def test_dsh_capture_metadata_still_reports_a_quarantine():
    async def run():
        server = ManifestServer()
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            relay = SyncRelay(Mock(), _stack(http))
            try:
                await relay.quarantine(SESSION, EXTERNAL, 10, ConnectorIngestRejectedError("refused"))
                for task in list(relay.retry_tasks):
                    task.cancel()
                await relay.operation({"kind": "snapshot.begin", "sessionId": SESSION, "snapshotId": "c",
                                       "throughSeq": 11, "meta": {"externalSessionId": EXTERNAL,
                                       "sourceState": {"availability": "available"}}})
                meta = next(note for note in server.notes if note["method"] == "session.meta.upsert")
                assert meta["params"]["sourceState"]["availability"] == "unavailable"
            finally:
                await relay.close()
    asyncio.run(run())


def test_an_unconfirmed_page_stops_the_step_and_replaces_at_once():
    server = ManifestServer()
    server.features = False
    _seed(server, [_item("a", 1), _item("b", 2)])
    new = [_item("b", 1, size=20 * 1024), _item("x", 2, size=20 * 1024), _item("y", 3, size=20 * 1024)]
    _run(server, [_snapshot(new)], budget=48 * 1024)
    reconcile_pages = [note for note in server.notes if note["params"].get("orderAuthoritative")]
    assert len(reconcile_pages) == 1, "no page after the first unconfirmed one"
    assert server.notes[-1]["params"]["complete"] is True
    assert server.ids() == ["b", "x", "y"]


def test_a_snapshot_naming_several_sessions_is_sent_whole():
    server = ManifestServer()
    other = {**_item("o", 2), "source": {"runtime": "codex", "sessionId": "another-native"}}
    _run(server, [_snapshot([_item("a", 1), other])])
    assert server.manifest_reads == 0
    assert server.notes[0]["params"]["complete"] is True


def test_dsh_capture_too_large_whole_is_reconciled_once_the_backend_can():
    async def run():
        server = ManifestServer()
        server.manifest_status = 404
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            client = _client(http)
            relay = SyncRelay(Mock(), _stack(http, client=client))
            try:
                items = make_items(11, 5 * MIB)
                await capture(relay, items)
                assert relay.quarantined[SESSION].source_state["reason"] == "history_too_large"
                # The backend is upgraded and the cooldown is over.
                server.manifest_status = None
                client._reconcile_disabled_until = 0.0
                await capture(relay, items)
                assert not relay.quarantined
                assert server.ids() == [item["id"] for item in items]
                restated = server.notes[-1]
                assert restated["method"] == "session.source.updated"
                assert restated["params"]["availability"] == "available"
                assert restated["params"]["observationOrigin"] == "event"
            finally:
                await relay.close()
    asyncio.run(run())


def test_dsh_manifest_waits_for_notifications_queued_before_it():
    async def run():
        server = ManifestServer()
        _seed(server, _dsh_items(2, 1024))
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            client = _client(http)
            relay = SyncRelay(Mock(), _stack(http, client=client))
            try:
                stale = {**make_items(3, 1024)[2], "id": "queued-before"}
                await client.enqueue("timeline.itemUpsert", {"sessionId": SESSION, "item": stale})
                await capture(relay, _dsh_items(2, 1024))
                assert server.ids() == ["item-0", "item-1"], "the queued item was stored first, then deleted"
            finally:
                await relay.close()
    asyncio.run(run())


def test_a_snapshot_mixing_named_and_anonymous_items_is_sent_whole():
    server = ManifestServer()
    anonymous = {**_item("anon", 2), "source": {"runtime": "codex"}}
    _run(server, [_snapshot([_item("a", 1), anonymous])])
    assert server.manifest_reads == 0
    assert server.notes[0]["params"]["complete"] is True


def test_dsh_page_with_an_anonymous_item_falls_back_to_the_whole_path():
    async def run():
        server = ManifestServer()
        async with httpx.AsyncClient(transport=httpx.MockTransport(server)) as http:
            relay = SyncRelay(Mock(), _stack(http))
            try:
                items = _dsh_items(3, 1024)
                items[1] = {**items[1], "source": {"runtime": "dsh"}}
                await capture(relay, items)
                # Pages before it were reconciled; the anonymous item never is.
                reconciled = [item["id"] for note in server.notes if note["params"].get("orderAuthoritative")
                              for item in note["params"]["items"]]
                assert items[1]["id"] not in reconciled
                assert "timeline.delete" not in server.methods()
                assert any(note["params"].get("complete") is True for note in server.notes)
                assert server.ids() == [item["id"] for item in items]
            finally:
                await relay.close()
    asyncio.run(run())
