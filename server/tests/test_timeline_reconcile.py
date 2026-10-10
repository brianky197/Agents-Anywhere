"""Issue #278: a complete snapshot reconciled as appends and deletes.

Instead of one replacing request, the Connector reads the stored manifest,
uploads only items that differ (with ``orderAuthoritative``) and deletes the
IDs the Runtime no longer has. These tests use a temporary SQLite database.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx
from test_backend_mvp import (
    create_connector_and_session,
    make_client,
    session_view_for_assertions,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "connector"))

from connector.server.ingest import ConnectorIngestClient


def _item(session_id: str, name: str, order_seq: int, text: str | None = None) -> dict[str, Any]:
    text = name if text is None else text
    return {
        "id": name,
        "sessionId": session_id,
        "type": "message",
        "status": "done",
        "role": "assistant",
        "content": {"text": text, "format": "markdown"},
        "source": {
            "runtime": "codex",
            "sessionId": "thr_1",
            "itemId": name,
            "itemType": "agentMessage",
        },
        "orderSeq": order_seq,
        "revision": 1,
        "contentHash": f"sha256:{name}:{text}",
    }


def _ingest(client, token: str, *notifications: dict[str, Any]) -> dict[str, Any]:
    response = client.post(
        "/connector/ingest",
        headers={"Authorization": f"Bearer {token}"},
        json={"notifications": list(notifications)},
    )
    assert response.status_code == 200, response.text
    assert response.json()["rejected"] == [], response.text
    return response.json()


def _sync(session_id: str, items: list[dict[str, Any]], **flags: Any) -> dict[str, Any]:
    return {
        "method": "timeline.sync",
        "params": {"sessionId": session_id, "runtime": "codex", "items": items, **flags},
    }


def _delete(session_id: str, item_ids: list[str]) -> dict[str, Any]:
    return {
        "method": "timeline.delete",
        "params": {"sessionId": session_id, "runtime": "codex", "itemIds": item_ids},
    }


def _manifest(client, token: str, session_id: str, **body: Any):
    return client.post(
        "/connector/timeline/manifest",
        headers={"Authorization": f"Bearer {token}"},
        json={"sessionId": session_id, "runtime": "codex", **body},
    )


def _ids(client, session_id: str, headers: dict[str, str]) -> list[str]:
    return [item["id"] for item in session_view_for_assertions(client, session_id, headers)["items"]]


def _seq(client, session_id: str, headers: dict[str, str]) -> int:
    response = client.get(f"/sessions/{session_id}/meta", headers=headers)
    response.raise_for_status()
    return response.json()["session"]["updatedSeq"]


def _snapshot_required(client, session_id: str, headers: dict[str, str], after: int) -> bool:
    response = client.get(
        f"/sessions/{session_id}/events",
        headers=headers,
        params={"after": f"seq:{after}"},
    )
    response.raise_for_status()
    return response.json()["snapshotRequired"]


def test_manifest_pages_ids_order_and_hashes(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, _ = create_connector_and_session(client)
    items = [_item(session_id, f"m{index}", index + 1) for index in range(5)]
    _ingest(client, token, _sync(session_id, items, complete=True))

    first = _manifest(client, token, session_id, limit=2)
    assert first.status_code == 200, first.text
    assert first.json() == {
        "items": [
            {"id": "m0", "orderSeq": 1, "contentHash": "sha256:m0:m0"},
            {"id": "m1", "orderSeq": 2, "contentHash": "sha256:m1:m1"},
        ],
        "next": {"orderSeq": 2, "id": "m1"},
    }
    seen = [entry["id"] for entry in first.json()["items"]]
    cursor = first.json()["next"]
    while cursor is not None:
        page = _manifest(client, token, session_id, limit=2, after=cursor).json()
        seen.extend(entry["id"] for entry in page["items"])
        cursor = page["next"]
    assert seen == [item["id"] for item in items]


def test_manifest_of_a_session_the_server_does_not_have_is_empty(tmp_path):
    client = make_client(tmp_path)
    _, token, _, _ = create_connector_and_session(client)

    response = _manifest(client, token, "not-created-yet")

    assert response.status_code == 200, response.text
    assert response.json() == {"items": [], "next": None}


def test_manifest_requires_a_connector_token(tmp_path):
    client = make_client(tmp_path)
    _, _, session_id, headers = create_connector_and_session(client)

    response = client.post(
        "/connector/timeline/manifest",
        headers=headers,
        json={"sessionId": session_id, "runtime": "codex"},
    )

    assert response.status_code == 401


def test_appended_items_in_order_are_plain_updates(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    _ingest(client, token, _sync(session_id, [_item(session_id, "a", 1)], orderAuthoritative=True))
    before = _seq(client, session_id, headers)

    _ingest(
        client,
        token,
        _sync(session_id, [_item(session_id, "b", 2), _item(session_id, "c", 3)], orderAuthoritative=True),
    )

    assert _ids(client, session_id, headers) == ["a", "b", "c"]
    assert _snapshot_required(client, session_id, headers, before) is False


def test_authoritative_order_moves_existing_items_and_resets_readers(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    _ingest(
        client,
        token,
        _sync(session_id, [_item(session_id, name, index + 1) for index, name in enumerate("abc")], complete=True),
    )
    before = _seq(client, session_id, headers)

    # Without the flag, an incremental sync keeps the stored order.
    _ingest(client, token, _sync(session_id, [_item(session_id, "c", 0)]))
    assert _ids(client, session_id, headers) == ["a", "b", "c"]

    _ingest(client, token, _sync(session_id, [_item(session_id, "c", 0)], orderAuthoritative=True))

    assert _ids(client, session_id, headers) == ["c", "a", "b"]
    assert _snapshot_required(client, session_id, headers, before) is True


def test_inserting_before_the_end_resets_readers(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    _ingest(
        client,
        token,
        _sync(session_id, [_item(session_id, "a", 1), _item(session_id, "c", 3)], complete=True),
    )
    before = _seq(client, session_id, headers)

    _ingest(client, token, _sync(session_id, [_item(session_id, "b", 2)], orderAuthoritative=True))

    assert _ids(client, session_id, headers) == ["a", "b", "c"]
    assert _snapshot_required(client, session_id, headers, before) is True


def test_delete_removes_known_ids_once_and_resets_readers(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    _ingest(
        client,
        token,
        _sync(session_id, [_item(session_id, name, index + 1) for index, name in enumerate("abc")], complete=True),
    )
    before = _seq(client, session_id, headers)

    _ingest(client, token, _delete(session_id, ["b", "unknown"]))

    assert _ids(client, session_id, headers) == ["a", "c"]
    assert _snapshot_required(client, session_id, headers, before) is True
    after_delete = _seq(client, session_id, headers)

    # Repeating it changes nothing.
    _ingest(client, token, _delete(session_id, ["b", "unknown"]))
    assert _seq(client, session_id, headers) == after_delete
    assert _snapshot_required(client, session_id, headers, after_delete) is False


def test_delete_rejects_a_malformed_id_list(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, _ = create_connector_and_session(client)

    response = client.post(
        "/connector/ingest",
        headers={"Authorization": f"Bearer {token}"},
        json={"notifications": [_delete(session_id, [])]},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "invalid_timeline_delete"


def test_reconcile_matches_a_complete_replacement(tmp_path):
    """Diffing against the manifest ends where a replacing snapshot would."""

    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    old = [_item(session_id, name, index + 1) for index, name in enumerate("abcde")]
    _ingest(client, token, _sync(session_id, old, complete=True))
    new = [
        _item(session_id, "a", 1),
        _item(session_id, "c", 2, text="edited"),
        _item(session_id, "x", 3),
        _item(session_id, "e", 4),
        _item(session_id, "b", 5),
    ]

    manifest = {
        entry["id"]: (entry["orderSeq"], entry["contentHash"])
        for entry in _manifest(client, token, session_id).json()["items"]
    }
    changed = [item for item in new if manifest.get(item["id"]) != (item["orderSeq"], item["contentHash"])]
    removed = sorted(manifest.keys() - {item["id"] for item in new})
    _ingest(client, token, _sync(session_id, changed, orderAuthoritative=True), _delete(session_id, removed))

    assert [item["id"] for item in changed] == ["c", "x", "e", "b"]
    assert removed == ["d"]
    stored = session_view_for_assertions(client, session_id, headers)["items"]
    assert [item["id"] for item in stored] == ["a", "c", "x", "e", "b"]
    assert [item["orderSeq"] for item in stored] == [1, 2, 3, 4, 5]
    assert stored[1]["content"]["text"] == "edited"


def test_connector_reconciles_a_snapshot_the_ingress_would_refuse(tmp_path):
    """The real Connector ingest client against the real Server, behind a size limit."""

    ingress_limit = 256 * 1024
    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    sent: list[tuple[str, int, int]] = []
    notes: list[dict[str, Any]] = []

    def ingress(request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/v2")
        if len(request.content) > ingress_limit:
            sent.append((path, len(request.content), 413))
            return httpx.Response(413)
        response = client.post(
            path,
            content=request.content,
            headers={"Authorization": request.headers["Authorization"], "Content-Type": "application/json"},
        )
        sent.append((path, len(request.content), response.status_code))
        if path == "/connector/ingest":
            notes.extend(json.loads(request.content)["notifications"])
        return httpx.Response(
            response.status_code, content=response.content, headers={"Content-Type": "application/json"}
        )

    async def access_token(force: bool) -> str:
        return token

    def snapshot(items: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "method": "timeline.sync",
            "params": {"sessionId": session_id, "runtime": "codex", "complete": True, "items": items},
        }

    def large(name: str, order_seq: int, text: str = "") -> dict[str, Any]:
        return _item(session_id, name, order_seq, text=(text or name) + " " + "历史" * 10_000)

    first = [large(f"m{index}", index + 1) for index in range(10)]
    second = [
        first[0],
        {**first[2], "orderSeq": 2},
        {**first[1], "orderSeq": 3},
        large("m3", 4, text="edited"),
        *first[5:],
        large("new", 11),
    ]

    async def upload(items: list[dict[str, Any]]) -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(ingress)) as http:
            ingest = ConnectorIngestClient("http://testserver", access_token, lambda: http, lambda _: http)
            ingest._max_body_bytes = 128 * 1024
            await ingest.ingest_notifications([snapshot(items)])

    asyncio.run(upload(first))
    assert _ids(client, session_id, headers) == [item["id"] for item in first]
    sent.clear()
    notes.clear()
    before = _seq(client, session_id, headers)
    asyncio.run(upload(second))
    assert _snapshot_required(client, session_id, headers, before) is True

    stored = session_view_for_assertions(client, session_id, headers)["items"]
    assert [item["id"] for item in stored] == [item["id"] for item in second]
    assert stored[3]["content"]["text"].startswith("edited")
    assert all(status == 200 for _, _, status in sent)
    assert max(size for _, size, _ in sent) <= ingress_limit
    # Unchanged items stay on the Server: only the moved, edited and new ones travel.
    uploaded = [item["id"] for note in notes if note["method"] == "timeline.sync" for item in note["params"]["items"]]
    deleted = [item_id for note in notes if note["method"] == "timeline.delete" for item_id in note["params"]["itemIds"]]
    assert uploaded == ["m2", "m1", "m3", "new"]
    assert deleted == ["m4"]
    assert not any(note["params"].get("complete") for note in notes)


def test_ingest_responses_confirm_reconcile_writes(tmp_path):
    client = make_client(tmp_path)
    _, token, session_id, _ = create_connector_and_session(client)

    body = _ingest(client, token, _delete(session_id, ["none"]))

    assert body["features"] == ["timeline.reconcile"]


def test_manifest_includes_items_accepted_but_not_yet_stored(tmp_path):
    """A replacing snapshot would remove a buffered item, so the manifest lists it."""

    client = make_client(tmp_path)
    _, token, session_id, _ = create_connector_and_session(client)
    _ingest(client, token, _sync(session_id, [_item(session_id, "a", 1)], complete=True))
    _ingest(
        client,
        token,
        {"method": "timeline.itemUpsert", "params": {"sessionId": session_id, "runtime": "codex",
                                                      "item": _item(session_id, "live", 2)}},
    )

    ids = [entry["id"] for entry in _manifest(client, token, session_id).json()["items"]]

    assert ids == ["a", "live"]


def test_reconcile_writes_resolve_the_session_they_name(tmp_path):
    """Pages of changed items resolve the session by externalSessionId, not by their items."""

    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client)
    external = client.get(f"/sessions/{session_id}/meta", headers=headers).json()["session"]["externalSessionId"]
    alias = "alias-the-server-does-not-know"
    anonymous = {**_item(session_id, "x", 1), "source": {"runtime": "codex", "itemId": "x"}}

    _ingest(client, token, _sync(alias, [anonymous], orderAuthoritative=True, externalSessionId=external))
    assert _ids(client, session_id, headers) == ["x"]

    manifest = _manifest(client, token, alias, externalSessionId=external).json()
    assert [entry["id"] for entry in manifest["items"]] == ["x"]

    delete = _delete(alias, ["x"])
    delete["params"]["externalSessionId"] = external
    _ingest(client, token, delete)
    assert _ids(client, session_id, headers) == []


def test_restating_the_hosts_source_fact_keeps_it(tmp_path):
    """What the DSH relay sends during a quarantine: the Host's own fact, not none."""

    client = make_client(tmp_path)
    _, token, session_id, headers = create_connector_and_session(client, runtime="dsh")
    archived = {"availability": "archived", "reason": "archived_in_dsh",
                "observedAt": "2026-10-10T10:00:00Z", "observationOrigin": "event"}
    _ingest(client, token, {"method": "session.source.updated",
                            "params": {"sessionId": session_id, "runtime": "dsh", **archived}})

    def source() -> tuple[str, str | None, str | None]:
        session = client.get(f"/sessions/{session_id}/meta", headers=headers).json()["session"]
        return session["sourceAvailability"], session["sourceAvailabilityReason"], session["sourceAvailabilityUpdatedAt"]

    before = source()
    assert before[0] == "archived"

    _ingest(client, token, {"method": "session.meta.upsert",
                            "params": {"sessionId": session_id, "runtime": "dsh", "sourceState": archived}})
    assert source() == before

    # Without a source fact, DSH metadata is read as available as of now.
    _ingest(client, token, {"method": "session.meta.upsert", "params": {"sessionId": session_id, "runtime": "dsh"}})
    assert source()[0] != "archived"
