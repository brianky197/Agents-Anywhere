from __future__ import annotations

import asyncio
import pickle
import sys
import tempfile
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from connector.logging import logger
from connector.runtime_protocol.host import RuntimeHostClient
from connector.runtime_protocol.models import (
    SessionSourceObservation,
    SessionSourceState,
)
from connector.runtimes.dsh.bridge.client import BridgeClient
from connector.runtimes.dsh.bridge.models import (
    capability_set,
    model_catalog,
    permission_catalog,
    session_meta,
    session_state,
    timeline_item,
)
from connector.runtimes.dsh.bridge.models import notice as session_notice
from connector.server.ingest import ConnectorIngestRejectedError
from connector.server.ingest_batching import ConnectorIngestSizeError
from connector.server.ingest_reconcile import (
    changed_items,
    delete_notifications,
    item_session_id,
)

_DURABLE_NOTIFICATIONS = {
    "timeline.itemUpsert", "session.meta.upsert", "session.state.updated",
    "session.source.updated", "session.turnEnded",
    "session.inventory.begin", "session.inventory.complete",
}


# The backend refused this session's history. It must not hold back the other
# sessions. A capture over the size limit cannot succeed again; any other refusal
# (an HTTP 200 rejection also covers server-side failures such as a database
# error) is retried with a fresh capture after a backoff.
_SESSION_INGEST_ERRORS = (ConnectorIngestSizeError, ConnectorIngestRejectedError, httpx.HTTPStatusError)
REJECTED_RETRY_DELAY_SECONDS = 30.0
REJECTED_RETRY_MAX_DELAY_SECONDS = 30 * 60.0
# History for a quarantined session would land on a timeline the backend never
# received a baseline for. Its metadata and state still flow.
_QUARANTINED_HISTORY = {"timeline.itemUpsert", "session.turnEnded"}
_SOURCE_FACT = ("availability", "reason", "observedAt", "observationOrigin")


@dataclass
class _Reconcile:
    """A capture reconciled against the backend's stored timeline (issue #278).

    Each page uploads only the items that differ, as it arrives, so the commit
    ACK (the bridge gives up after 60 seconds) never waits for the whole
    history. The commit then deletes stored items the capture does not have.
    """

    manifest: dict[str, tuple[int, str]] | None
    external_session_id: str | None = None
    seen: set[str] = field(default_factory=set)
    # The backend has no manifest, or a page was refused: the commit sends the
    # spooled capture as one complete timeline.sync instead.
    failed: bool = False
    # Why a reconcile write was refused, when one was.
    error: Exception | None = None


@dataclass
class _Quarantine:
    external_session_id: str
    through_seq: Any
    source_state: dict[str, Any]
    # Not a size refusal: request a new capture after a backoff.
    retryable: bool = False
    attempts: int = 1
    # The Host's latest source fact while quarantined is not "available"
    # (for example archived); a recovery must not override it.
    host_fact: bool = False
    # That fact as reported, with its own observation time.
    host_source: dict[str, Any] | None = None


def _too_large(error: Exception | None) -> bool:
    return isinstance(error, ConnectorIngestSizeError) or (
        isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 413)


def _checkpoint(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    seq, fingerprint = value.get("throughSeq"), value.get("historyHash")
    if (value.get("version") != 1 or value.get("projectionVersion") not in (2, 3)
        or type(seq) is not int or not -1 <= seq <= 9007199254740991
        or type(value.get("settled")) is not bool or not isinstance(fingerprint, str)
        or len(fingerprint) != 64 or any(c not in "0123456789abcdef" for c in fingerprint)):
        return None
    return {key: value[key] for key in ("version", "projectionVersion", "throughSeq", "historyHash", "settled")}


class SyncRelay:
    """Reassemble transport pages, then forward existing platform notifications.

    No native event parsing or backend-specific persistence protocol lives here.
    Checkpoint-capable feeds await ingestion for history-bearing notifications.
    Transport page ACKs alone never advance the Connector's persisted state.
    """

    def __init__(
        self, client: BridgeClient, host: RuntimeHostClient, *, retry_delay: float = 1.0,
        rejected_retry_delay: float = REJECTED_RETRY_DELAY_SECONDS,
    ) -> None:
        self.client, self.host = client, host
        self.queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=2)
        self.stream_id: str | None = None
        self.retry_delay = retry_delay
        self.rejected_retry_delay = rejected_retry_delay
        self.retry_tasks: set[asyncio.Task[None]] = set()
        self.task: asyncio.Task[None] | None = None
        self.snapshot: dict[str, Any] | None = None
        self.file = None
        self.spool = ExitStack()
        self.items: list[dict[str, Any]] = []
        self.item_bytes = 0
        self.item_ids: set[str] = set()
        self.durable_checkpoints = False
        self.loaded_checkpoint: dict[str, Any] | None = None
        # Platform session ID -> rejected capture. Survives resubscription, so
        # an unchanged oversized capture is not uploaded again; a restart
        # retries it.
        self.quarantined: dict[str, _Quarantine] = {}
        self.reconcile: _Reconcile | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.run(), name="dsh-event-sync")

    def accept(self, payload: Mapping[str, Any]) -> None:
        try:
            self.queue.put_nowait(dict(payload))
        except asyncio.QueueFull:
            self.restart()

    def restart(self, stream_id: str | None = None) -> None:
        if stream_id and self.stream_id and stream_id != self.stream_id:
            return
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queue.put_nowait(None)

    async def close(self) -> None:
        if self.task and self.task is not asyncio.current_task():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        retries = list(self.retry_tasks)
        for task in retries:
            task.cancel()
        await asyncio.gather(*retries, return_exceptions=True)
        self.clear_snapshot()

    def clear_snapshot(self) -> None:
        self.spool.close()
        self.file, self.snapshot = None, None
        # Items already uploaded stay: they are current items of the session.
        self.reconcile = None
        self.item_ids.clear()
        self.items = []
        self.item_bytes = 0

    async def operation(self, op: dict[str, Any]) -> None:
        kind = op.get("kind")
        if kind in {"checkpoint.load", "checkpoint.save", "checkpoint.delete"}:
            if not self.durable_checkpoints or self.snapshot is not None:
                raise ValueError("Checkpoint operation outside a committed sync boundary")
            external_id = op.get("externalSessionId")
            if not isinstance(external_id, str) or not external_id:
                raise ValueError("Checkpoint requires an external session identity")
            key = f"dsh/sync/checkpoints/{external_id}"
            if kind == "checkpoint.load":
                self.loaded_checkpoint = _checkpoint(await self.host.sync_state_read(key))
            elif kind == "checkpoint.delete":
                await self.host.sync_state_delete(key)
            else:
                checkpoint = _checkpoint(op.get("checkpoint"))
                if checkpoint is None:
                    raise ValueError("Invalid DSH checkpoint")
                if any(q.external_session_id == external_id for q in self.quarantined.values()):
                    # The Host believes its snapshot was ingested; it was not.
                    # Never let a later subscription resume from that boundary.
                    return
                # All preceding history operations were synchronously ingested.
                # The Host's periodic/final flush uses the same JSON as scanners.
                await self.host.sync_state_write(key, checkpoint)
        elif kind == "snapshot.begin":
            self.clear_snapshot()
            self.snapshot = op
            await self.begin_reconcile(op["sessionId"], op)

        elif kind in {"snapshot.items", "snapshot.commit", "snapshot.abort"}:
            if not self.snapshot or any(op.get(k) != self.snapshot.get(k) for k in ("snapshotId", "sessionId")):
                raise ValueError("Snapshot page has a different capture identity")
            if kind == "snapshot.items":
                for raw in op["items"]:
                    item = timeline_item(raw)
                    if item.session_id != op["sessionId"] or item.id in self.item_ids:
                        raise ValueError("Invalid snapshot item identity")
                    self.item_ids.add(item.id)
                await self.store_items(op["items"])
                await self.reconcile_page(op["sessionId"], op["items"])
            elif kind == "snapshot.commit":
                if len(self.item_ids) != op.get("totalItems") or op.get("throughSeq") != self.snapshot["throughSeq"]:
                    raise ValueError("Incomplete snapshot; previous backend history remains intact")
                await self.commit_snapshot(op["sessionId"], self.snapshot)
                self.clear_snapshot()
            else:
                self.clear_snapshot()
        elif kind == "notifications":
            pending: list[dict[str, Any]] = []
            for raw in op["notifications"]:
                notice = self.unquarantined(raw)
                if notice is None:
                    continue
                if self.durable_checkpoints and notice.get("method") in _DURABLE_NOTIFICATIONS:
                    params = notice.get("params")
                    if not isinstance(params, dict):
                        raise ValueError("Invalid runtime notification")
                    if notice["method"] == "timeline.itemUpsert":
                        item = timeline_item(params["item"])
                        if item.session_id != params.get("sessionId"):
                            raise ValueError("Incremental item belongs to a different session")
                    pending.append(notice)
                else:
                    await self.ingest_notifications(pending)
                    pending = []
                    await self.publish_notification(notice)
            await self.ingest_notifications(pending)
        elif kind == "workspace.inventory":
            # Older plugin builds sent native project facts. Ignore those batches;
            # all project grouping and naming use the existing session cwd path.
            return
        else:
            raise ValueError(f"Unsupported bridge operation: {kind}")

    def unchanged_refusal(self, session_id: str, snapshot: dict[str, Any]) -> _Quarantine | None:
        """The quarantine of an identical capture that was too large."""
        rejected = self.quarantined.get(session_id)
        if rejected is not None and not rejected.retryable and rejected.through_seq == snapshot["throughSeq"]:
            return rejected
        return None

    async def begin_reconcile(self, session_id: str, snapshot: dict[str, Any]) -> None:
        """Read the backend's timeline before the first page arrives.

        It happens within the begin ACK, so the pages' ACKs only wait for their
        own changes. DSH items name the capture's native session in
        ``source.sessionId``; every reconcile write names it too.
        """
        # A capture refused as too large is tried again here: reconciled, it
        # may fit. Without a manifest, or when a reconcile write is itself too
        # large, the commit only restates metadata.
        external = snapshot["meta"].get("externalSessionId")
        external = external if isinstance(external, str) and external else None
        manifest = await self.host.read_timeline_manifest(
            "dsh", {"sessionId": session_id, **({"externalSessionId": external} if external else {})})
        self.reconcile = _Reconcile(manifest, external, failed=manifest is None)
        if manifest is None:
            return
        try:
            # The items need their session; an unknown one drops them.
            await self.host.publish_runtime_notifications(
                "dsh", [self.quarantined_meta(self.meta_notice(session_id, snapshot))])
        except _SESSION_INGEST_ERRORS as error:
            self.reconcile.failed, self.reconcile.error = True, error
            logger.info("DSH snapshot metadata refused; the commit sends it whole error={}", error)

    async def reconcile_page(self, session_id: str, items: list[dict[str, Any]]) -> None:
        """Upload what one snapshot page changes on the backend's timeline."""
        reconcile = self.reconcile
        if reconcile is None or reconcile.failed or reconcile.manifest is None:
            return
        if any(item_session_id(item) != reconcile.external_session_id for item in items):
            # Every item must name the manifest's session, or none may: a
            # Server too old for pinned resolution resolves a page by its items.
            reconcile.failed = True
            logger.info("DSH snapshot items name another session; the commit sends it whole")
            return
        reconcile.seen.update(item["id"] for item in items)
        changed = changed_items(items, reconcile.manifest)
        if not changed:
            return
        try:
            await self.host.publish_runtime_notifications("dsh", [{"method": "timeline.sync", "params": {
                "sessionId": session_id, "externalSessionId": reconcile.external_session_id,
                "complete": False, "orderAuthoritative": True, "items": changed}}])
        except _SESSION_INGEST_ERRORS as error:
            # A network failure still fails the feed, as for any history.
            reconcile.failed, reconcile.error = True, error
            logger.info("DSH snapshot page refused; the commit sends it whole error={}", error)

    async def commit_reconcile(self, session_id: str, meta_notice: dict[str, Any]) -> bool:
        """Delete what the capture no longer has; False leaves it to the whole path."""
        reconcile = self.reconcile
        if reconcile is None or reconcile.failed or reconcile.manifest is None:
            return False
        removed = [item_id for item_id in reconcile.manifest if item_id not in reconcile.seen]
        try:
            await self.host.publish_runtime_notifications("dsh", [
                meta_notice,
                *delete_notifications({"sessionId": session_id}, reconcile.external_session_id, removed),
            ])
        except _SESSION_INGEST_ERRORS as error:
            reconcile.error = error
            logger.info("DSH snapshot commit refused; sending it whole error={}", error)
            return False
        return True

    @staticmethod
    def meta_notice(session_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {"method": "session.meta.upsert", "params": {"sessionId": session_id, **snapshot["meta"]}}

    async def commit_snapshot(self, session_id: str, snapshot: dict[str, Any]) -> None:
        meta = snapshot["meta"]
        meta_notice = self.meta_notice(session_id, snapshot)
        reconciled = await self.commit_reconcile(session_id, meta_notice)
        # An unchanged capture once refused as too large is skipped only when it
        # could not be reconciled: no manifest, or a write that was itself too
        # large. Any other refusal while reconciling goes through the whole path,
        # whose outcome replaces the old quarantine.
        reconcile = self.reconcile
        retry = reconcile is not None and reconcile.manifest is not None and not _too_large(reconcile.error)
        rejected = None if reconciled or retry else self.unchanged_refusal(session_id, snapshot)
        if rejected is not None:
            # Same capture as the oversized one. Keep the metadata current without
            # uploading the history again or reporting the source as available.
            meta_notice["params"]["sourceState"] = rejected.source_state
            await self.host.publish_runtime_notifications("dsh", [meta_notice])
            return
        if not reconciled:
            try:
                # The metadata goes alone and first: while it is refused the history
                # is not uploaded, and a refusal of the history (413 in particular)
                # is not reported as the metadata's.
                await self.host.publish_runtime_notifications("dsh", [meta_notice])
                # Reuse the existing complete snapshot API only after every page is
                # received. The ingest client reconciles it against the manifest too.
                items = await self.load_items()
                await self.host.publish_runtime_notifications("dsh", [
                    {"method": "timeline.sync", "params": {"sessionId": session_id,
                        "externalSessionId": meta["externalSessionId"], "items": items, "complete": True}},
                ])
            except _SESSION_INGEST_ERRORS as error:
                await self.quarantine(session_id, meta["externalSessionId"], snapshot["throughSeq"], error)
                return
        released = self.quarantined.get(session_id)
        source = meta.get("sourceState")
        if released is not None and not released.host_fact and isinstance(source, dict):
            # The capture's source fact carries its capture time, which can be
            # older than an unavailable observation reported while its pages
            # arrived; the Server keeps the newer one. Restate it as of now.
            # A transient failure leaves the quarantine to the next capture.
            try:
                await self.host.publish_runtime_notifications("dsh", [{
                    "method": "session.source.updated",
                    "params": {"sessionId": session_id, "externalSessionId": meta["externalSessionId"],
                               "observationOrigin": "event", **source, "observedAt": _observed_now()},
                }])
            except _SESSION_INGEST_ERRORS as error:
                logger.warning("DSH source restatement refused session_id={} error={}", session_id, error)
        self.quarantined.pop(session_id, None)

    async def quarantine(self, session_id: str, external_id: str, through_seq: Any, error: Exception) -> None:
        """Isolate one session whose history the backend refused.

        The commit is still acknowledged so the feed moves on to the other
        sessions, but no checkpoint is saved and later history for this session
        is held back until a new capture is accepted. Unless the capture was
        too large, a new one is requested after a growing delay.
        """
        too_large = _too_large(error)
        reason = "history_too_large" if too_large else "history_rejected"
        source_state = {"availability": "unavailable", "reason": reason,
                        "observedAt": _observed_now(), "observationOrigin": "event"}
        previous = self.quarantined.get(session_id)
        record = _Quarantine(external_id, through_seq, source_state, retryable=not too_large,
                             attempts=previous.attempts + 1 if previous is not None else 1,
                             host_fact=previous is not None and previous.host_fact,
                             host_source=previous.host_source if previous is not None else None)
        self.quarantined[session_id] = record
        logger.warning("DSH session history quarantined session_id={} reason={} attempts={} error={}",
                       session_id, reason, record.attempts, error)
        await self.host.publish_runtime_notifications("dsh", [{"method": "session.source.updated", "params": {
            "sessionId": session_id, "externalSessionId": external_id, **source_state}}])
        if record.retryable:
            task = asyncio.create_task(self.retry_capture(session_id, record), name="dsh-sync-retry")
            self.retry_tasks.add(task)
            task.add_done_callback(self.retry_tasks.discard)

    async def retry_capture(self, session_id: str, record: _Quarantine) -> None:
        """Ask the Host for a new complete capture of a refused session."""
        attempts = record.attempts
        while True:
            await asyncio.sleep(min(self.rejected_retry_delay * 2 ** (attempts - 1), REJECTED_RETRY_MAX_DELAY_SECONDS))
            if self.quarantined.get(session_id) is not record:
                return  # Released, or a newer refusal owns the retry.
            try:
                await self.client.request("runtime.sync.refresh", {
                    "sessionId": session_id, "externalSessionId": record.external_session_id})
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - keep retrying this session only
                logger.warning("DSH session history retry failed session_id={} error_type={}",
                               session_id, type(error).__name__)
                attempts += 1
                continue
            return

    def quarantined_meta(self, notice: dict[str, Any]) -> dict[str, Any]:
        """A capture's metadata, still reporting its session's quarantine.

        Unlike ``unquarantined`` it leaves the Host's latest fact alone: the
        capture's source fact may be older than one reported since.
        """
        params = notice["params"]
        rejected = self.quarantined.get(params["sessionId"])
        source = params.get("sourceState")
        if rejected is None or not isinstance(source, dict) or source.get("availability") != "available":
            return notice
        if rejected.host_fact:
            # The Host reported something else since. Restate that fact as it
            # was observed, so the backend keeps it: metadata without a source
            # fact would make the backend infer an available one as of now.
            if rejected.host_source is None:
                return notice
            return {**notice, "params": {**params, "sourceState": rejected.host_source}}
        return {**notice, "params": {**params, "sourceState": {**rejected.source_state, "observedAt": _observed_now()}}}

    def source_state_for(self, session_id: str, source: dict[str, Any]) -> dict[str, Any]:
        """The source fact to report for a session, honouring its quarantine."""
        rejected = self.quarantined.get(session_id)
        if rejected is None:
            return source
        # Track the Host's latest fact: available again after archived means
        # a recovery may restate the capture's source.
        rejected.host_fact = source.get("availability") != "available"
        if rejected.host_fact:
            rejected.host_source = dict(source)
            return source
        return {**rejected.source_state, "observedAt": _observed_now()}

    def unquarantined(self, notice: dict[str, Any]) -> dict[str, Any] | None:
        """Drop or rewrite a Host notification that would contradict a quarantine."""
        if not self.quarantined:
            return notice
        method, params = notice.get("method"), notice.get("params")
        if not isinstance(params, dict):
            return notice
        if (rejected := self._quarantine_for(params)) is not None:
            if method in _QUARANTINED_HISTORY:
                return None
            if method == "session.source.updated":
                rejected.host_fact = params.get("availability") != "available"
                if rejected.host_fact:
                    rejected.host_source = {key: params[key] for key in _SOURCE_FACT if key in params}
                if not rejected.host_fact:
                    return None
            if method == "session.meta.upsert" and isinstance(params.get("sourceState"), dict):
                source = self.source_state_for(params["sessionId"], params["sourceState"])
                return {**notice, "params": {**params, "sourceState": source}}
        if method == "session.inventory.complete" and isinstance(params.get("sessions"), list):
            sessions = [
                {**entry, "sourceState": rejected.source_state}
                if (rejected := self._quarantine_for(entry)) is not None else entry
                for entry in params["sessions"]
            ]
            return {**notice, "params": {**params, "sessions": sessions}}
        return notice

    def _quarantine_for(self, params: Any) -> _Quarantine | None:
        session_id = params.get("sessionId") if isinstance(params, dict) else None
        return self.quarantined.get(session_id) if isinstance(session_id, str) else None

    async def ingest_notifications(self, notifications: list[dict[str, Any]]) -> None:
        if not notifications:
            return
        await self.host.publish_runtime_notifications("dsh", notifications)
        if any(n["method"] == "session.inventory.complete" and n["params"].get("complete") is True for n in notifications):
            await self.host.runtime_health_update("running")

    async def store_items(self, items: list[dict[str, Any]]) -> None:
        self.item_bytes += object_bytes(items)
        if self.file is None and self.item_bytes <= 8 * 1024 * 1024:
            self.items.extend(items)
            return
        # Keep only one capture in RAM. Pickle is internal, never accepted from a peer.
        # Wait for disk work even on cancellation before close_snapshot can close the file.
        def write() -> None:
            if self.file is None:
                self.file = self.spool.enter_context(tempfile.TemporaryFile(mode="w+b"))  # noqa: SIM115 - capture lifetime spans pages
                pickle.dump(self.items, self.file, protocol=pickle.HIGHEST_PROTOCOL)
                self.items = []
            pickle.dump(items, self.file, protocol=pickle.HIGHEST_PROTOCOL)
        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def load_items(self) -> list[dict[str, Any]]:
        if self.file is None:
            return self.items
        task = asyncio.create_task(asyncio.to_thread(self.read_spool))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def read_spool(self) -> list[dict[str, Any]]:
        self.file.seek(0)
        items: list[dict[str, Any]] = []
        while True:
            try:
                items.extend(pickle.load(self.file))
            except EOFError:
                return items

    async def publish_notification(self, notice: dict[str, Any]) -> None:
        method, params = notice.get("method"), notice.get("params")
        if not isinstance(params, dict):
            raise ValueError("Invalid runtime notification")  # noqa: TRY004 - protocol validation error
        # Use the existing typed Host API, just like Codex and Claude. The Host
        # owns instance binding, coalescing, WebSocket delivery and HTTP fallback.
        if method == "timeline.itemUpsert":
            item = timeline_item(params["item"])
            if item.session_id != params.get("sessionId"):
                raise ValueError("Incremental item belongs to a different session")
            await self.host.timeline_item_upsert(item)
        elif method == "session.meta.upsert":
            meta = session_meta(params)
            await self.host.session_meta_upsert(
                session_id=meta.session_id, runtime="dsh", external_session_id=meta.external_session_id,
                title=meta.title, cwd=meta.cwd, ordering_time=meta.ordering_time, metadata=meta.metadata,
            )
        elif method == "session.state.updated":
            state = session_state(params)
            await self.host.session_state_update(
                session_id=state.session_id, runtime="dsh", external_session_id=state.external_session_id,
                status=state.status, selections=state.selections, status_reason=state.status_reason,
                error=state.error, metadata=state.metadata,
            )
        elif method == "session.source.updated":
            await self.host.session_source_update(SessionSourceObservation(
                session_id=params["sessionId"], external_session_id=params.get("externalSessionId"), runtime="dsh",
                state=SessionSourceState(availability=params["availability"], reason=params.get("reason"),
                    observed_at=params.get("observedAt"), observation_origin=params.get("observationOrigin", "event")),
            ))
        elif method == "session.turnEnded":
            await self.host.session_turn_ended(
                session_id=params["sessionId"], runtime="dsh", external_session_id=params.get("externalSessionId"),
                turn_id=params.get("turnId"), outcome=params.get("outcome", "completed"), metadata=params.get("metadata"),
            )
        elif method == "notice.upsert":
            await self.host.notice_upsert(session_notice(params))
        elif method == "runtime.capability.updated":
            await self.host.runtime_capabilities_update(capability_set(params, connector_id=self.host.connector_id))
        elif method == "session.capability.updated":
            await self.host.session_capabilities_update(capability_set(params, connector_id=self.host.connector_id))
        elif method == "catalog.model.update":
            await self.host.model_catalog_update(model_catalog(params))
        elif method == "catalog.permission.update":
            await self.host.permission_catalog_update(permission_catalog(params))
        elif method in {"session.inventory.begin", "session.inventory.complete"}:
            await self.host.publish_runtime_notifications("dsh", [notice])
            if method == "session.inventory.complete" and params.get("complete") is True:
                await self.host.runtime_health_update("running")
        else:
            raise ValueError(f"Unsupported runtime notification: {method}")

    async def run(self) -> None:
        try:
            while True:
                try:
                    await self.consume()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - isolate and recover a failed feed
                    logger.warning("DSH event sync interrupted; resubscribing for history calibration ({})", type(error).__name__)
                    await self.host.runtime_health_update("starting", {
                        "code": "runtime_sync_interrupted", "message": "DSH 会话同步中断，正在重试…", "retryable": True,
                    })
                    if not self.client.connected:
                        return
                    self.clear_snapshot()
                    await asyncio.sleep(self.retry_delay)
                    while not self.queue.empty():
                        self.queue.get_nowait()
        finally:
            self.clear_snapshot()

    async def consume(self) -> None:
        # Subscription replaces only this feed. Concurrent RPC requests keep
        # their connection and are never cancelled by an ingest/sync failure.
        try:
            await self.host.runtime_health_update("starting", {
                "code": "runtime_initializing", "message": "正在同步 DSH 会话…", "retryable": True,
            })
            subscription = await self.client.request("runtime.sync.subscribe", {"checkpointVersion": 1})
            projection_version = subscription.get("projectionVersion")
            if projection_version not in (2, 3):
                raise ValueError("Unsupported DSH projection version")
            stream_id, expected = subscription["streamId"], 1
            self.durable_checkpoints = subscription.get("checkpointVersion") == 1
            self.stream_id = stream_id
            while True:
                batch = await self.queue.get()
                if batch is None:
                    raise RuntimeError("The DSH sync stream needs a new subscription")
                if batch.get("streamId") != stream_id:
                    continue
                if batch.get("batchSeq") != expected:
                    raise ValueError("Out-of-order event batch; reconnect to recalibrate")
                if batch.get("projectionVersion") != projection_version or not isinstance(batch.get("operations"), list) or not batch["operations"]:
                    raise ValueError("Invalid DSH event batch")
                self.loaded_checkpoint = None
                for operation in batch["operations"]:
                    await self.operation(operation)
                ack = {"streamId": stream_id, "batchSeq": expected}
                if any(op.get("kind") == "checkpoint.load" for op in batch["operations"]):
                    ack["checkpoint"] = self.loaded_checkpoint
                await self.client.request("runtime.sync.ack", ack)
                expected += 1
        finally:
            self.clear_snapshot()


def _observed_now() -> str:
    # Same shape as the Host's Date.toISOString(): the backend orders
    # observations by comparing these strings.
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def object_bytes(value: Any) -> int:
    """Conservative retained size for decoded JSON, without serializing it again."""
    if isinstance(value, dict):
        return sys.getsizeof(value) + sum(sys.getsizeof(key) + object_bytes(item) for key, item in value.items())
    if isinstance(value, list):
        return sys.getsizeof(value) + sum(object_bytes(item) for item in value)
    return sys.getsizeof(value)
