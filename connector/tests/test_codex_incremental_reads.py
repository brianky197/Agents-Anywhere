"""Exercise native SQLite guards, RPC paging and durable Connector checkpoints."""
import asyncio
import copy
import ctypes
import hashlib
import json
import os
import sqlite3
import sys
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest
from test_codex_runtime import FakeCodexClient, _config

from connector.runtimes.codex.runtime import CodexRuntime
from connector.runtimes.codex.sdk.runtime_client import CodexThreadTurnsPage
from connector.runtimes.codex.sessions import history_index as codex_history_index
from connector.runtimes.codex.sessions.history_index import read_history_index
from connector.server.runtime_host import ConnectorRuntimeHost
from connector.server.sync_state import JsonSyncStateStore


class NativeHistory(FakeCodexClient):
    def __init__(self, root, count=240):
        super().__init__()
        self.path = root / "sessions/2026/09/17/thread.jsonl"
        self.path.parent.mkdir(parents=True)
        self.path.write_text(json.dumps({"type": "session_meta", "payload": {
            "id": "thread_1", "history_mode": "paginated"}}) + "\n")
        self.db_path = root / "thread_history_1.sqlite"
        with sqlite3.connect(self.db_path) as db:
            db.executescript("""
                CREATE TABLE thread_history_projection_state (thread_id TEXT, next_rollout_byte_offset INTEGER);
                CREATE TABLE thread_turns (thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER, status TEXT,
                    error_json TEXT, started_at INTEGER, completed_at INTEGER, duration_ms INTEGER);
                CREATE TABLE thread_items (thread_id TEXT, turn_id TEXT, item_id TEXT, rollout_ordinal INTEGER,
                    updated_at_ordinal INTEGER, created_at_ms INTEGER, item_type TEXT);
            """)
        self.turns = [self.turn(n) for n in range(count)]
        self.revisions = {}
        self.pages = []
        self.on_page = None
        self.persist()

    @staticmethod
    def turn(n):
        return {"id": f"turn_{n}", "status": "completed", "items": [
            {"id": f"item_{n}", "type": "agentMessage", "text": f"answer {n}"}]}

    def persist(self):
        with self.path.open("a") as stream:
            stream.write(json.dumps({"type": "event_msg", "payload": {"type": "test_projection_update"}}) + "\n")
        with sqlite3.connect(self.db_path) as db:
            db.execute("DELETE FROM thread_turns")
            db.execute("DELETE FROM thread_items")
            db.execute("DELETE FROM thread_history_projection_state")
            db.execute("INSERT INTO thread_history_projection_state VALUES (?, ?)", ("thread_1", self.path.stat().st_size))
            for n, turn in enumerate(self.turns):
                db.execute("INSERT INTO thread_turns VALUES (?, ?, ?, ?, NULL, 1, 2, 1000)", ("thread_1", turn["id"], n * 100, turn["status"]))
                for pos, item in enumerate(turn["items"]):
                    db.execute("INSERT INTO thread_items VALUES (?, ?, ?, ?, ?, ?, ?)", (
                        "thread_1", turn["id"], item["id"], n * 100 + pos + 1,
                        self.revisions.get(item["id"], n * 100 + pos + 1), 1, item["type"]))
        self.ref = {"id": "thread_1", "path": str(self.path), "updatedAt": 1, "historyMode": "paginated"}
        self.results["thread/list"] = {"threads": [self.ref]}
        self.results["thread/read"] = {"thread": {**self.ref, "turns": self.turns}}

    async def list_thread_turns_page(self, thread_id, cursor=None, limit=20):
        self.pages.append(cursor)
        start = int(cursor or 0)
        turns = copy.deepcopy(list(reversed(self.turns))[start:start + limit])
        if self.on_page:
            callback, self.on_page = self.on_page, None
            callback()
        return CodexThreadTurnsPage(tuple(turns), str(start + limit) if start + limit < len(self.turns) else None)


def boot(path, client):
    store = JsonSyncStateStore(path)
    host = ConnectorRuntimeHost("conn_test", AsyncMock(), AsyncMock(), store)
    return CodexRuntime(config=_config(), host=host, client=client), host, store


async def prepare(runtime):
    session = (await runtime.list_sessions())[0]
    return await runtime.prepare_session_timeline_sync(session.session_id, "thread_1")


def test_restart_reads_zero_history_and_append_reads_one_page_with_retry(tmp_path):
    async def run():
        client = NativeHistory(tmp_path / "native")
        runtime, _host, store = boot(tmp_path / "checkpoint.json", client)
        initial = await prepare(runtime)
        assert len(initial.snapshot.items) == 240
        assert len(client.pages) == 12
        await initial.commit()
        store.flush()
        runtime, _host, store = boot(tmp_path / "checkpoint.json", client)
        client.pages.clear()
        unchanged = await prepare(runtime)
        assert unchanged.snapshot is None
        assert client.pages == []
        assert not any(method == "thread/read" for method, _ in client.requests)
        client.turns.append(client.turn(240))
        client.persist()
        added = await prepare(runtime)
        assert len(client.pages) == 1
        assert len(added.snapshot.items) == 1
        assert added.snapshot.items[0].order_seq == 240
        assert added.snapshot.complete is False
        # No ingestion ACK: the durable checkpoint must still describe the old history.
        retry = await prepare(runtime)
        assert retry.snapshot.items == added.snapshot.items
        await retry.commit()
        store.flush()
        runtime, _host, _ = boot(tmp_path / "checkpoint.json", client)
        client.pages.clear()
        assert (await prepare(runtime)).snapshot is None
        assert client.pages == []
    asyncio.run(run())


def test_owned_native_in_progress_turn_is_exposed_as_running_when_index_lags(
    tmp_path,
    monkeypatch,
):
    async def run():
        client = NativeHistory(tmp_path / "native", count=2)
        client.turns[-1]["status"] = "inProgress"
        client.persist()
        with client.path.open("a") as stream:
            stream.write("{}\n")
        os.utime(client.path, (1, 1))
        monkeypatch.setattr(
            codex_history_index,
            "_source_is_open_by_codex",
            lambda _source: True,
        )
        assert read_history_index(client.ref) is None

        runtime, _, _ = boot(tmp_path / "checkpoint.json", client)
        session = (await runtime.list_sessions())[0]
        state = await runtime.get_session_state(
            session.session_id,
            external_session_id="thread_1",
        )

        assert state is not None
        assert state.status == "running"
        assert state.metadata["source"] == "codex.thread_history.state"
        assert state.metadata["turn_id"] == "turn_1"
        assert state.metadata["native_activity_evidence"] == "owner"

    asyncio.run(run())


def test_stale_unowned_native_in_progress_turn_is_exposed_as_idle(tmp_path):
    async def run():
        client = NativeHistory(tmp_path / "native", count=2)
        client.turns[-1]["status"] = "inProgress"
        client.persist()
        os.utime(client.path, (1, 1))

        runtime, _, _ = boot(tmp_path / "checkpoint.json", client)
        session = (await runtime.list_sessions())[0]
        state = await runtime.get_session_state(
            session.session_id,
            external_session_id="thread_1",
        )

        assert state is not None
        assert state.status == "idle"
        assert state.metadata["source"] == "codex.thread_history.state"
        assert state.metadata["turn_id"] == "turn_1"
        assert state.metadata["native_status"] == "inProgress"

    asyncio.run(run())


@pytest.mark.skipif(sys.platform != "win32", reason="Windows file-system ownership query")
def test_windows_rollout_open_in_codex_is_owned_without_walking_processes(
    tmp_path,
    monkeypatch,
):
    def walk_processes(*_args, **_kwargs):
        raise AssertionError("walking every process's handles stalled the Connector on Windows")

    async def run():
        client = NativeHistory(tmp_path / "native", count=2)
        client.turns[-1]["status"] = "inProgress"
        client.persist()
        os.utime(client.path, (1, 1))
        monkeypatch.setattr(codex_history_index.psutil, "process_iter", walk_processes)
        runtime, _, _ = boot(tmp_path / "checkpoint.json", client)
        session = (await runtime.list_sessions())[0]

        async def state():
            return await runtime.get_session_state(session.session_id, external_session_id="thread_1")

        this_process = codex_history_index.psutil.Process().name()
        rollout = client.path.open("ab")
        try:
            # Another program holding the rollout is not Codex running the thread.
            held_elsewhere = await state()
            with monkeypatch.context() as patch:
                patch.setattr(codex_history_index, "_is_codex_process", lambda info: info.get("name") == this_process)
                owned = await state()
                rollout.close()
                released = await state()
        finally:
            rollout.close()

        assert owned is not None and owned.status == "running"
        assert owned.metadata["native_activity_evidence"] == "owner"
        for idle in (held_elsewhere, released):
            assert idle is not None and idle.status == "idle"
            assert idle.metadata["native_activity_evidence"] == "stale"

    asyncio.run(run())


@pytest.mark.skipif(sys.platform != "win32", reason="Windows file-system ownership query")
def test_windows_unanswered_ownership_query_claims_no_activity(tmp_path, monkeypatch):
    client = NativeHistory(tmp_path / "native", count=2)
    client.turns[-1]["status"] = "inProgress"
    client.persist()
    os.utime(client.path, (1, 1))
    assert codex_history_index.read_native_thread_activity(client.ref).evidence == "stale"

    create_file, close_handle, _query = codex_history_index._windows_file_api()
    not_supported = 0xC00000BB
    monkeypatch.setattr(
        codex_history_index,
        "_windows_file_api",
        lambda: (create_file, close_handle, lambda *_args: not_supported),
    )

    # A file system that cannot say who holds the rollout must not turn a
    # possibly live turn into a stale one.
    assert codex_history_index._windows_processes_using_file(str(client.path)) is None
    assert codex_history_index.read_native_thread_activity(client.ref) is None


@pytest.mark.skipif(sys.platform != "win32", reason="Windows file-system ownership query")
def test_windows_ownership_query_never_contends_for_the_rollout(tmp_path, monkeypatch):
    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("{}\n")
    using = codex_history_index._windows_processes_using_file
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.restype = ctypes.c_void_p
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    invalid = (None, ctypes.c_void_p(-1).value)

    def open_exclusively():
        return kernel32.CreateFileW(str(rollout), 0x40000000, 0, None, 3, 0, None)

    assert os.getpid() not in using(str(rollout))
    assert using(str(tmp_path / "missing.jsonl")) is None
    with rollout.open("rb"):
        assert os.getpid() in using(str(rollout))

    # A holder that shares nothing still shows up.
    exclusive = open_exclusively()
    assert exclusive not in invalid
    try:
        with pytest.raises(PermissionError):
            rollout.open("rb")
        assert os.getpid() in using(str(rollout))
    finally:
        kernel32.CloseHandle(exclusive)
    assert os.getpid() not in using(str(rollout))

    # While the query's own handle is open, Codex can still open the rollout
    # exclusively, and the file can still be renamed.
    create_file, close_handle, query = codex_history_index._windows_file_api()
    others = []

    def query_while_others_use_the_rollout(*args):
        writer = open_exclusively()
        others.append(writer not in invalid)
        if writer not in invalid:
            kernel32.CloseHandle(writer)
        moved = rollout.with_name("moved.jsonl")
        os.replace(rollout, moved)
        os.replace(moved, rollout)
        return query(*args)

    monkeypatch.setattr(
        codex_history_index,
        "_windows_file_api",
        lambda: (create_file, close_handle, query_while_others_use_the_rollout),
    )
    assert using(str(rollout)) is not None
    assert others == [True]


@pytest.mark.parametrize("mutation", ["old_edit", "old_tool_edit", "delete_turn", "delete_item", "reorder", "unfinished", "invalid_checkpoint"])
def test_incremental_checkpoint_matches_full_projection_after_mutation(tmp_path, mutation):
    async def run():
        client = NativeHistory(tmp_path / "native", count=65)
        if mutation in {"old_tool_edit", "delete_item"}:
            client.turns[5]["items"].append({"id": "tool", "type": "commandExecution", "command": "echo hi", "status": "completed", "aggregatedOutput": "hi"})
        if mutation == "unfinished":
            client.turns[-1]["status"] = "inProgress"
        client.persist()
        runtime, host, store = boot(tmp_path / "checkpoint.json", client)
        initial = await prepare(runtime)
        await initial.commit()
        store.flush()
        runtime, host, _ = boot(tmp_path / "checkpoint.json", client)
        client.pages.clear()
        if mutation == "old_edit":
            client.turns[5]["items"][0]["text"] = "edited old response"
            client.revisions["item_5"] = 99999
        elif mutation == "old_tool_edit":
            client.turns[5]["items"][-1]["aggregatedOutput"] = "changed old tool output"
            client.revisions["tool"] = 99999
        elif mutation == "delete_turn":
            client.turns.pop(5)
        elif mutation == "delete_item":
            client.turns[5]["items"].pop()
        elif mutation == "reorder":
            client.turns[3], client.turns[4] = client.turns[4], client.turns[3]
        elif mutation == "unfinished":
            client.turns[-1]["items"][0]["text"] = "finished"
            client.turns[-1]["status"] = "completed"
            client.revisions["item_64"] = 99999
        else:
            checkpoint = await host.sync_state_read("codex/timeline-sync/thread_1")
            checkpoint["readState"]["counts"] = {}
            await host.sync_state_write("codex/timeline-sync/thread_1", checkpoint)
        client.persist()
        delta = await prepare(runtime)
        if mutation.startswith("delete"):
            assert delta.snapshot.complete is True
        await delta.commit()
        session = (await runtime.list_sessions())[0]
        full = await runtime.get_session_snapshot(session.session_id, "thread_1")
        expected = {item.id: hashlib.sha256(json.dumps(asdict(item), sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
                    for item in full.items if item.type not in {"turn.start", "turn.end"}}
        assert (await host.sync_state_read("codex/timeline-sync/thread_1"))["items"] == expected
        if mutation in {"old_edit", "old_tool_edit"}:
            assert len(client.pages) > 1, "a tail-only heuristic missed an older mutation"
    asyncio.run(run())


@pytest.mark.parametrize("problem", ["schema", "lag", "fork", "missing_file"])
def test_unknown_or_stale_native_index_falls_back_to_full_rpc(tmp_path, problem):
    async def run():
        client = NativeHistory(tmp_path / "native", count=5)
        if problem == "schema":
            with sqlite3.connect(client.db_path) as db:
                db.execute("DROP TABLE thread_items")
        elif problem == "lag":
            with client.path.open("a") as stream:
                stream.write("{}\n")
        elif problem == "fork":
            client.path.write_text(json.dumps({"type": "session_meta", "payload": {
                "id": "thread_1", "history_mode": "paginated", "history_base": {"source_thread_id": "parent"}}}) + "\n")
            client.persist()
        else:
            client.path.unlink()
        assert read_history_index(client.ref) is None
        runtime, _, _ = boot(tmp_path / "checkpoint.json", client)
        result = await prepare(runtime)
        assert len(result.snapshot.items) == 5
        assert client.pages == []
        assert any(method == "thread/read" for method, _ in client.requests)
    asyncio.run(run())


def test_history_changing_between_pages_does_not_commit_checkpoint(tmp_path):
    async def run():
        client = NativeHistory(tmp_path / "native")
        runtime, host, _ = boot(tmp_path / "checkpoint.json", client)
        client.on_page = lambda: client.persist()
        with pytest.raises(RuntimeError, match="changed while reading"):
            await prepare(runtime)
        assert await host.sync_state_read("codex/timeline-sync/thread_1") is None
        result = await prepare(runtime)
        assert len(result.snapshot.items) == 240
    asyncio.run(run())
