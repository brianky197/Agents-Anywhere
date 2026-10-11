from __future__ import annotations

import asyncio

import pytest

from agent_server.core.events import (
    EventCursorError,
    capability_event_semantic_fingerprint,
    event_cursor,
    events_from_invalidation,
    parse_event_cursor,
    protocol_event,
    revisions_are_complete,
)
from agent_server.core.models import SessionRuntimeState, SessionStatus, TimelineItemIn
from agent_server.infra.connector_rpc import ConnectorRpcManager
from agent_server.infra.db.migrations import upgrade_database
from agent_server.infra.redis_coordinator import RedisCoordinator
from agent_server.infra.repositories.facade import Store
from agent_server.infra.timeline_broker import TimelineBroker
from agent_server.services.event_recovery import EventRecoveryService
from agent_server.services.session_meta_projection import persist_runtime_state
from agent_server.services.session_runtime_state_cache import SessionRuntimeStateCache
from agent_server.services.timeline_write_buffer import TimelineWriteBuffer
from session_fixtures import create_session_with_project


def test_event_cursor_is_a_strict_durable_revision_token() -> None:
    assert event_cursor(12) == "seq:12"
    assert parse_event_cursor("seq:12") == 12

    for invalid in ("12", "seq:-1", "seq:+1", "seq:01", "seq:"):
        with pytest.raises(EventCursorError):
            parse_event_cursor(invalid)


def test_capability_event_fingerprint_ignores_set_revision_and_record_order() -> None:
    first = protocol_event(
        "session-1",
        sequence=7,
        event_type="runtime.capability.updated",
        payload={
            "capabilitySet": {
                "revision": 7,
                "capabilities": [
                    {
                        "capabilityId": "session.send_message",
                        "runtimeId": "runtime-1",
                        "available": True,
                    },
                    {
                        "capabilityId": "session.interrupt",
                        "available": False,
                    },
                ],
            }
        },
    )
    reordered = protocol_event(
        "session-1",
        sequence=8,
        event_type="runtime.capability.updated",
        payload={
            "capabilitySet": {
                "revision": 99,
                "capabilities": list(
                    reversed(first.payload["capabilitySet"]["capabilities"])
                ),
            }
        },
    )
    changed_runtime = protocol_event(
        "session-1",
        sequence=8,
        event_type="runtime.capability.updated",
        payload={
            "capabilitySet": {
                "revision": 99,
                "capabilities": [
                    {
                        **first.payload["capabilitySet"]["capabilities"][0],
                        "runtimeId": "runtime-2",
                    },
                    first.payload["capabilitySet"]["capabilities"][1],
                ],
            }
        },
    )

    assert capability_event_semantic_fingerprint(first) == (
        capability_event_semantic_fingerprint(reordered)
    )
    assert capability_event_semantic_fingerprint(first) != (
        capability_event_semantic_fingerprint(changed_runtime)
    )
    assert (
        capability_event_semantic_fingerprint(
            protocol_event(
                "session-1",
                sequence=8,
                event_type="session.meta.updated",
                payload={"session": {}},
            )
        )
        is None
    )


def test_timeline_reset_invalidation_becomes_one_snapshot_event() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 4,
            "timelineReset": True,
            "items": [
                {
                    "id": "item-1",
                    "updatedSeq": 3,
                    "revision": 1,
                }
            ],
        }
    )

    assert len(events) == 1
    assert events[0].type == "timeline.snapshot"
    assert events[0].sequence == 4
    assert events[0].payload["items"][0]["id"] == "item-1"


def test_empty_timeline_reset_becomes_one_empty_snapshot_event() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 4,
            "timelineReset": True,
            "items": [],
        }
    )

    assert len(events) == 1
    assert events[0].type == "timeline.snapshot"
    assert events[0].sequence == 4
    assert events[0].payload["items"] == []


def test_notice_reset_invalidation_becomes_one_snapshot_event() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 5,
            "noticesReset": True,
            "notices": [],
        }
    )

    event_types = {event.type for event in events}
    assert event_types == {"runtime.notice.snapshot"}
    for event in events:
        assert event.payload["notices"] == []


def test_runtime_state_invalidation_emits_runtime_state_event() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 7,
            "runtimeState": {
                "sessionId": "session-1",
                "runtime": "codex",
                "status": "running",
                "selections": {"model": "sel_model"},
                "updatedSeq": 7,
            },
        }
    )

    assert len(events) == 1
    assert events[0].type == "runtime.state.updated"
    assert events[0].payload["state"]["status"] == "running"


def test_session_invalidation_emits_meta_event() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 9,
            "session": {
                "id": "session-1",
                "title": "Updated",
                "status": "idle",
                "updatedSeq": 9,
            },
        }
    )

    event_types = {event.type for event in events}
    assert "session.meta.updated" in event_types
    assert "session.status_changed" not in event_types
    meta_event = next(event for event in events if event.type == "session.meta.updated")
    assert meta_event.payload["session"]["title"] == "Updated"


def test_notice_invalidation_emits_runtime_notice_update() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 8,
            "notices": [
                {
                    "id": "notice-1",
                    "status": "answered",
                    "revision": 2,
                    "updatedSeq": 8,
                }
            ],
        }
    )

    event_types = {event.type for event in events}
    assert event_types == {"runtime.notice.updated"}
    runtime_event = next(
        event for event in events if event.type == "runtime.notice.updated"
    )
    assert runtime_event.payload["notice"]["id"] == "notice-1"


def test_notice_invalidation_adds_event_sequence_for_live_notice() -> None:
    events = events_from_invalidation(
        {
            "sessionId": "session-1",
            "nextSeq": 8,
            "notices": [
                {
                    "noticeId": "notice-1",
                    "type": "interaction",
                    "sessionId": "session-1",
                    "title": "Approve command",
                    "status": "open",
                }
            ],
        }
    )

    assert len(events) == 1
    assert events[0].type == "runtime.notice.updated"
    assert events[0].sequence == 8
    assert events[0].payload["notice"]["updatedSeq"] == 8


def test_recovery_requires_every_durable_revision_to_be_projected() -> None:
    events = [
        protocol_event(
            "session-1",
            sequence=2,
            event_type="timeline.item_updated",
            payload={"item": {"id": "item-1"}},
        )
    ]

    assert not revisions_are_complete(
        after_sequence=0,
        current_sequence=2,
        events=events,
    )
    assert revisions_are_complete(
        after_sequence=1,
        current_sequence=2,
        events=events,
    )


def test_timeline_snapshot_replace_requires_snapshot_for_deleted_items(
    tmp_path,
) -> None:
    async def exercise() -> None:
        path = tmp_path / "events.sqlite3"
        upgrade_database(sqlite_path=path)
        store = Store(path)
        presence = ConnectorRpcManager()
        try:
            connector, _token, _prefix = await store.create_connector(
                name="dev",
                user_id="user-1",
            )
            session = await create_session_with_project(
                store,
                connector_id=connector.id,
                user_id="user-1",
                external_session_id="thread-1",
                title="Recovery",
            )
            first = _timeline_item(session.id, "item-1", 1)
            second = _timeline_item(session.id, "item-2", 2)
            await store.upsert_timeline_item(session_id=session.id, item=first)
            await store.upsert_timeline_item(session_id=session.id, item=second)
            before_replace = await store.get_session_seq(session.id)

            await store.replace_timeline_snapshot(
                session_id=session.id,
                items=[first],
            )
            recovery = await EventRecoveryService(store, presence).recover(
                session.id,
                after=event_cursor(before_replace),
                user_id="user-1",
            )

            assert recovery.snapshotRequired is True
        finally:
            await presence.close()
            await store.close()

    asyncio.run(exercise())


def test_recovery_returns_runtime_state_changed_before_subscription(
    tmp_path,
) -> None:
    async def exercise() -> None:
        path = tmp_path / "events.sqlite3"
        upgrade_database(sqlite_path=path)
        store = Store(path)
        presence = ConnectorRpcManager()
        cache = SessionRuntimeStateCache()
        try:
            connector, _token, _prefix = await store.create_connector(
                name="dev",
                user_id="user-1",
            )
            session = await create_session_with_project(
                store,
                connector_id=connector.id,
                user_id="user-1",
                external_session_id="thread-1",
                title="Recovery",
            )
            await _put_runtime_status(store, cache, session.id, "running")
            # The client's snapshot shows the running turn.
            snapshot_cursor = event_cursor(await store.get_session_seq(session.id))
            # The turn ends before the client's WebSocket subscribes, so the
            # live runtime.state.updated reaches no subscriber.
            idle = await _put_runtime_status(store, cache, session.id, "idle")

            recovery = await EventRecoveryService(
                store,
                presence,
                runtime_state_cache=cache,
            ).recover(session.id, after=snapshot_cursor, user_id="user-1")

            states = [
                event for event in recovery.events if event.type == "runtime.state.updated"
            ]
            assert len(states) == 1
            assert states[0].sequence == parse_event_cursor(recovery.nextCursor)
            assert states[0].payload["state"]["status"] == "idle"
            assert states[0].payload["state"]["updatedSeq"] == idle.updatedSeq
            metas = [
                event for event in recovery.events if event.type == "session.meta.updated"
            ]
            assert [event.payload["session"]["status"] for event in metas] == ["idle"]
        finally:
            await presence.close()
            await store.close()

    asyncio.run(exercise())


def test_recovery_at_current_cursor_returns_cached_runtime_state(tmp_path) -> None:
    async def exercise() -> None:
        path = tmp_path / "events.sqlite3"
        upgrade_database(sqlite_path=path)
        store = Store(path)
        presence = ConnectorRpcManager()
        cache = SessionRuntimeStateCache()
        try:
            connector, _token, _prefix = await store.create_connector(
                name="dev",
                user_id="user-1",
            )
            session = await create_session_with_project(
                store,
                connector_id=connector.id,
                user_id="user-1",
                external_session_id="thread-1",
                title="Recovery",
            )
            await _put_runtime_status(store, cache, session.id, "idle")
            current = await store.get_session_seq(session.id)

            recovery = await EventRecoveryService(
                store,
                presence,
                runtime_state_cache=cache,
            ).recover(session.id, after=event_cursor(current), user_id="user-1")

            states = [
                event for event in recovery.events if event.type == "runtime.state.updated"
            ]
            assert [event.payload["state"]["status"] for event in states] == ["idle"]
            assert states[0].sequence == current
        finally:
            await presence.close()
            await store.close()

    asyncio.run(exercise())


def test_recovery_returns_runtime_state_older_than_the_cursor(tmp_path) -> None:
    """A snapshot reads the state before its cursor, so the cursor can pass it."""

    async def exercise() -> None:
        path = tmp_path / "events.sqlite3"
        upgrade_database(sqlite_path=path)
        store = Store(path)
        presence = ConnectorRpcManager()
        cache = SessionRuntimeStateCache()
        try:
            connector, _token, _prefix = await store.create_connector(
                name="dev",
                user_id="user-1",
            )
            session = await create_session_with_project(
                store,
                connector_id=connector.id,
                user_id="user-1",
                external_session_id="thread-1",
                title="Recovery",
            )
            await _put_runtime_status(store, cache, session.id, "running")
            idle = await _put_runtime_status(store, cache, session.id, "idle")
            # The snapshot's cursor already covers the idle revision although
            # its state was read while the turn was still running.
            snapshot_cursor = event_cursor(idle.updatedSeq)
            await store.upsert_timeline_item(
                session_id=session.id,
                item=_timeline_item(session.id, "item-1", 1),
            )
            assert await store.get_session_seq(session.id) > idle.updatedSeq

            recovery = await EventRecoveryService(
                store,
                presence,
                runtime_state_cache=cache,
            ).recover(session.id, after=snapshot_cursor, user_id="user-1")

            states = [
                event for event in recovery.events if event.type == "runtime.state.updated"
            ]
            assert len(states) == 1
            assert states[0].sequence == parse_event_cursor(recovery.nextCursor)
            assert states[0].payload["state"]["status"] == "idle"
            assert states[0].payload["state"]["updatedSeq"] == idle.updatedSeq
        finally:
            await presence.close()
            await store.close()

    asyncio.run(exercise())


def test_recovery_cannot_pair_a_persisted_status_with_an_older_cached_state(
    tmp_path,
) -> None:
    async def exercise() -> None:
        path = tmp_path / "events.sqlite3"
        upgrade_database(sqlite_path=path)
        store = Store(path)
        presence = ConnectorRpcManager()
        coordinator = RedisCoordinator()
        buffer = TimelineWriteBuffer(store, TimelineBroker(coordinator), coordinator)
        put_started = asyncio.Event()
        release_put = asyncio.Event()

        class SlowCache(SessionRuntimeStateCache):
            blocking = False

            async def put(self, state: SessionRuntimeState) -> None:
                if self.blocking:
                    put_started.set()
                    await release_put.wait()
                await super().put(state)

        cache = SlowCache()
        try:
            connector, _token, _prefix = await store.create_connector(
                name="dev",
                user_id="user-1",
            )
            session = await create_session_with_project(
                store,
                connector_id=connector.id,
                user_id="user-1",
                external_session_id="thread-1",
                title="Recovery",
            )
            running = await persist_runtime_state(
                store,
                cache,
                (await store.get_session_runtime_state(session.id)).model_copy(
                    update={"status": "running"}
                ),
            )
            snapshot_cursor = event_cursor(await store.get_session_seq(session.id))

            # A background runtime refresh persists idle and is about to share
            # it when recovery arrives.
            cache.blocking = True
            refresh = asyncio.create_task(
                persist_runtime_state(
                    store,
                    cache,
                    running.model_copy(update={"status": "idle"}),
                )
            )
            await put_started.wait()
            recovery_task = asyncio.create_task(
                EventRecoveryService(
                    store,
                    presence,
                    buffer,
                    runtime_state_cache=cache,
                ).recover(session.id, after=snapshot_cursor, user_id="user-1")
            )
            await asyncio.sleep(0.05)
            assert not recovery_task.done()
            release_put.set()
            idle = await refresh
            recovery = await recovery_task

            states = [
                event for event in recovery.events if event.type == "runtime.state.updated"
            ]
            assert [event.payload["state"]["status"] for event in states] == ["idle"]
            assert states[0].payload["state"]["updatedSeq"] == idle.updatedSeq
            metas = [
                event for event in recovery.events if event.type == "session.meta.updated"
            ]
            assert [event.payload["session"]["status"] for event in metas] == ["idle"]
        finally:
            await buffer.close()
            await presence.close()
            await store.close()

    asyncio.run(exercise())


async def _put_runtime_status(
    store: Store,
    cache: SessionRuntimeStateCache,
    session_id: str,
    status: SessionStatus,
) -> SessionRuntimeState:
    """Persist and cache a runtime status the way connector ingest does."""
    persisted = await store.set_session_status(session_id, status)
    state = (await store.get_session_runtime_state(session_id)).model_copy(
        update={
            "status": status,
            "updatedSeq": max(
                await store.get_session_seq(session_id),
                persisted.updatedSeq,
            ),
        }
    )
    await cache.put(state)
    return state


def _timeline_item(session_id: str, item_id: str, order_seq: int) -> TimelineItemIn:
    return TimelineItemIn.model_validate(
        {
            "id": item_id,
            "sessionId": session_id,
            "type": "message",
            "status": "done",
            "role": "assistant",
            "content": {"text": item_id},
            "source": {
                "runtime": "codex",
                "sessionId": "thread-1",
                "itemId": item_id,
            },
            "orderSeq": order_seq,
            "revision": 1,
            "contentHash": f"sha256:{item_id}",
        }
    )
