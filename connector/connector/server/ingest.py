from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from connector.logging import logger
from connector.server.auth import ConnectorAuthenticationError
from connector.server.errors import ConnectorNetworkError
from connector.server.ingest_batching import (
    MAX_INGEST_BODY_BYTES,
    ConnectorIngestSizeError,
    drop_delivered,
    ingest_body,
    iter_ingest_pages,
    next_ingest_page,
)
from connector.server.ingest_reconcile import (
    MANIFEST_PAGE_ITEMS,
    Manifest,
    is_full_snapshot,
    manifest_request,
    reconcile_snapshot,
)
from connector.server.urls import api_v2_url

# HTTP ingest owns explicit bulk sync and disconnected WebSocket fallback.
# History-bearing notifications such as `timeline.sync` must use ingest even
# when the WebSocket is connected; they can exceed the backend WebSocket frame
# limit and are not latency-sensitive. Live small notifications still travel
# over the connector WebSocket.
FLUSH_WINDOW_SECONDS = 0.02
FLUSH_MAX = 64
# A Server without the timeline manifest is asked again after this long, so an
# upgraded Server is noticed without restarting the Connector.
RECONCILE_RETRY_SECONDS = 10 * 60

AccessTokenProvider = Callable[[bool], Awaitable[str]]
HttpClientGetter = Callable[[], httpx.AsyncClient | None]
HttpClientFactory = Callable[[httpx.Timeout | float], httpx.AsyncClient]


class ConnectorIngestRejectedError(RuntimeError):
    """The backend accepted the HTTP request but rejected notifications inside it."""


# Retrying these cannot succeed. They reject only the affected notifications;
# the rest of a batch is still delivered.
PERMANENT_INGEST_ERRORS = (ConnectorIngestRejectedError, ConnectorIngestSizeError, httpx.HTTPStatusError)


class ConnectorIngestClient:
    def __init__(
        self,
        server_url: str,
        access_token_provider: AccessTokenProvider,
        http_client_getter: HttpClientGetter,
        http_client_factory: HttpClientFactory,
    ) -> None:
        self._server_url = server_url
        self._access_token_provider = access_token_provider
        self._http_client_getter = http_client_getter
        self._http_client_factory = http_client_factory
        self._notify_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1024)
        self._inflight: list[dict[str, Any]] = []
        self._retry_delay = 1.0
        self._closed: Exception | None = None
        self._post_lock = asyncio.Lock()
        self._available = asyncio.Event()
        self._posting = False
        self._max_body_bytes = MAX_INGEST_BODY_BYTES
        self._reconcile_disabled_until = 0.0

    @property
    def has_pending(self) -> bool:
        return self._posting or bool(self._inflight) or not self._notify_queue.empty()

    def close(self, error: Exception | None = None) -> None:
        self._closed = error or RuntimeError("connector ingest is closed")
        # Wake blocked producers. They check _closed again after admission.
        while not self._notify_queue.empty():
            self._notify_queue.get_nowait()


    async def enqueue(self, method: str, params: dict[str, Any]) -> None:
        if self._closed is not None:
            raise self._closed
        await self._notify_queue.put({"method": method, "params": params})
        self._available.set()
        if self._closed is not None:
            self.close(self._closed)
            raise self._closed

    async def ingest_notifications(self, notifications: list[dict[str, Any]]) -> None:
        """Send a batch synchronously, bypassing the flush queue."""
        if not notifications:
            return
        await self.post_batch(list(notifications))

    async def flush_loop(self) -> None:
        """Keep a failed batch at the head; cancellation leaves it recoverable.

        Delivery is at least once after an ambiguous HTTP failure. State/item
        replacements retain their protocol versions; no synthetic revision is
        introduced by a retry. Authentication and permanent rejections do not
        enter the transient-error retry loop.
        """
        attempt = 0
        while True:
            await self._available.wait()
            if not self._inflight:
                await asyncio.sleep(FLUSH_WINDOW_SECONDS)
            try:
                async with self._post_lock:
                    self._posting = True
                    try:
                        self._collect_pending()
                        await self._post_batch(self._inflight)
                        self._inflight = []
                        if self._notify_queue.empty():
                            self._available.clear()
                    finally:
                        self._posting = False
            except ConnectorAuthenticationError as exc:
                self.close(exc)
                raise
            except ConnectorNetworkError as exc:
                attempt += 1
                if attempt == 1 or attempt % 10 == 0:
                    logger.warning("connector ingest deferred notifications={} attempt={} error={}", len(self._inflight), attempt, exc)
                await asyncio.sleep(min(30.0, self._retry_delay * 2 ** min(attempt - 1, 5)))
                continue
            except PERMANENT_INGEST_ERRORS as exc:
                logger.warning("connector ingest permanently rejected queued notification error={}", exc)
            self._inflight = []
            attempt = 0

    def _collect_pending(self) -> None:
        if self._inflight:
            return
        while len(self._inflight) < FLUSH_MAX and not self._notify_queue.empty():
            self._inflight.append(self._notify_queue.get_nowait())

    async def post_batch(self, notifications: list[dict[str, Any]]) -> None:
        async with self._post_lock:
            self._posting = True
            try:
                # Direct scanner snapshots cannot overtake an older failed batch.
                # Drain only work already queued at admission so a live stream
                # cannot starve this synchronous snapshot indefinitely.
                remaining = len(self._inflight) + self._notify_queue.qsize()
                while remaining:
                    self._collect_pending()
                    if not self._inflight:
                        break
                    count = len(self._inflight)
                    try:
                        await self._post_batch(self._inflight)
                    except PERMANENT_INGEST_ERRORS as exc:
                        # This queued batch belongs to an earlier producer. Its
                        # permanent failure must not quarantine the direct caller.
                        logger.warning("queued ingest rejected; continuing direct sync error={}", exc)
                    remaining = max(0, remaining - count)
                    self._inflight = []
                await self._post_batch(notifications)
            finally:
                self._posting = False
                if not self.has_pending:
                    self._available.clear()

    async def _post_batch(self, pending: list[dict[str, Any]]) -> None:
        """Deliver ``pending`` in byte-bounded pages, in order.

        Accepted pages are removed from ``pending`` in place when this returns
        or raises, so a transient retry resumes at the first unaccepted page and
        never replays an accepted notification such as ``session.turnEnded``.
        A permanently rejected notification is skipped, the rest are still
        delivered, and then the first permanent error is raised.
        """
        pending[:] = coalesce_timeline_item_upserts(pending)
        if not pending:
            return
        token = [await self._access_token_provider(False)]
        client = self._http_client_getter()
        owned = client is None
        if client is None:
            client = self._http_client_factory(60)
        try:
            # A complete snapshot becomes appends and deletes against the
            # stored manifest (issue #278). The rewrite replaces it in
            # ``pending``, so a retry after a transient failure resumes it.
            await self._reconcile_snapshots(client, token, pending)
        except BaseException:
            if owned:
                await client.aclose()
            raise
        pages = iter_ingest_pages(pending, self._max_body_bytes)
        delivered = (0, 0)
        rejected: list[Exception] = []
        page_number = 0
        try:
            while (page := await asyncio.to_thread(next_ingest_page, pages)) is not None:
                body = page.body
                try:
                    await self._post_page(client, token, body)
                except ConnectorIngestSizeError as exc:
                    if len(page.fragments) == 1:
                        _record_rejection(rejected, exc, page.fragments[0].method)
                    else:
                        # The server limit is below our page budget. Retry each
                        # notification alone so only the oversized one is lost.
                        for fragment in page.fragments:
                            try:
                                await self._post_page(client, token, ingest_body([fragment.data]))
                            except PERMANENT_INGEST_ERRORS as fragment_error:
                                _record_rejection(rejected, fragment_error, fragment.method)
                            delivered = fragment.after
                except PERMANENT_INGEST_ERRORS as exc:
                    _record_rejection(rejected, exc, ",".join(f.method for f in page.fragments))
                delivered = page.after
                page_number += 1
                if len(body) >= 1024 * 1024 or page_number > 1:
                    logger.info("bounded ingest page accepted page={} bytes={}", page_number, len(body))
                if page.last:
                    # Allow the caller to clear inflight/commit without a new
                    # cancellation point after final acceptance.
                    break
        finally:
            drop_delivered(pending, delivered)
            if owned:
                await client.aclose()
        if rejected:
            raise rejected[0]

    def _reconcile_enabled(self) -> bool:
        return time.monotonic() >= self._reconcile_disabled_until

    async def read_timeline_manifest(self, params: dict[str, Any]) -> Manifest | None:
        """The stored timeline of one session, or None without a manifest."""
        if not self._reconcile_enabled():
            return None
        token = [await self._access_token_provider(False)]
        client = self._http_client_getter()
        owned = client is None
        if client is None:
            client = self._http_client_factory(60)
        try:
            return await self._read_manifest(client, token, params)
        finally:
            if owned:
                await client.aclose()

    async def _reconcile_snapshots(
        self, client: httpx.AsyncClient, token: list[str], pending: list[dict[str, Any]],
    ) -> None:
        if not any(is_full_snapshot(notification) for notification in pending):
            return
        rewritten: list[dict[str, Any]] = []
        for notification in pending:
            manifest = None
            if is_full_snapshot(notification) and self._reconcile_enabled():
                manifest = await self._read_manifest(client, token, notification["params"])
            if manifest is None:
                rewritten.append(notification)
            else:
                rewritten.extend(await asyncio.to_thread(reconcile_snapshot, notification, manifest))
        pending[:] = rewritten

    async def _read_manifest(
        self, client: httpx.AsyncClient, token: list[str], params: dict[str, Any],
    ) -> Manifest | None:
        """Read every manifest page; None means send the complete snapshot whole."""
        body = manifest_request(params)
        manifest: Manifest = {}
        after = None
        while True:
            request = {**body, "limit": MANIFEST_PAGE_ITEMS, **({"after": after} if after else {})}
            response = await self._post_json(client, token, "/connector/timeline/manifest", request)
            status = getattr(response, "status_code", 200)
            if status == 404:
                # The Server predates the manifest: complete snapshots go whole.
                self._disable_reconcile()
                return None
            if status in {408, 429} or status >= 500:
                raise ConnectorNetworkError(f"backend manifest temporarily unavailable: HTTP {status}")
            if status >= 400:
                # For example an unconfigured runtime: the snapshot itself gets
                # the same refusal as before.
                logger.warning("timeline manifest refused HTTP {}; sending the snapshot whole", status)
                return None
            payload = _response_json(response)
            entries = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(entries, list):
                self._disable_reconcile()
                return None
            for entry in entries:
                if (isinstance(entry, dict) and isinstance(entry.get("id"), str)
                        and type(entry.get("orderSeq")) is int and isinstance(entry.get("contentHash"), str)):
                    manifest[entry["id"]] = (entry["orderSeq"], entry["contentHash"])
            after = payload.get("next")
            if not isinstance(after, dict):
                return manifest

    def _disable_reconcile(self) -> None:
        if self._reconcile_enabled():
            logger.warning("backend has no timeline manifest; sending complete snapshots whole")
        self._reconcile_disabled_until = time.monotonic() + RECONCILE_RETRY_SECONDS

    async def _post_json(
        self, client: httpx.AsyncClient, token: list[str], path: str, payload: dict[str, Any],
    ) -> httpx.Response:
        url = api_v2_url(self._server_url, path)
        try:
            response = await client.post(url, headers={"Authorization": f"Bearer {token[0]}"},
                                         json=payload, timeout=60)
            if getattr(response, "status_code", None) == 401:
                token[0] = await self._access_token_provider(True)
                response = await client.post(url, headers={"Authorization": f"Bearer {token[0]}"},
                                             json=payload, timeout=60)
                if getattr(response, "status_code", None) == 401:
                    raise ConnectorAuthenticationError("connector credential no longer valid")
        except httpx.RequestError as exc:
            raise ConnectorNetworkError(f"backend manifest request failed: {type(exc).__name__}") from exc
        return response

    async def _post_page(self, client: httpx.AsyncClient, token: list[str], body: bytes) -> None:
        # Both an auth retry and an ambiguous network replay resend the exact
        # same bytes, so item IDs and order are retained.
        try:
            response = await self._post_ingest_batch(client, token[0], body)
        except httpx.RequestError as exc:
            raise ConnectorNetworkError(
                f"backend ingest request failed: {type(exc).__name__}"
            ) from exc
        if getattr(response, "status_code", None) == 401:
            logger.warning(
                "connector ingest token rejected; refreshing access token and retrying"
            )
            token[0] = await self._access_token_provider(True)
            try:
                response = await self._post_ingest_batch(client, token[0], body)
            except httpx.RequestError as exc:
                raise ConnectorNetworkError(
                    f"backend ingest retry failed: {type(exc).__name__}"
                ) from exc
            if getattr(response, "status_code", None) == 401:
                raise ConnectorAuthenticationError("connector credential no longer valid")
        status = getattr(response, "status_code", 200)
        if status == 413:
            raise ConnectorIngestSizeError(len(body), self._max_body_bytes, "http_413")
        if status in {408, 429} or status >= 500:
            raise ConnectorNetworkError(f"backend ingest temporarily unavailable: HTTP {status}")
        response.raise_for_status()
        _raise_for_rejected_notifications(response)

    async def _post_ingest_batch(
        self,
        client: httpx.AsyncClient,
        access_token: str,
        body: bytes,
    ) -> httpx.Response:
        return await client.post(
            api_v2_url(self._server_url, "/connector/ingest"),
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            content=body,
            timeout=60,
        )


def _record_rejection(rejected: list[Exception], error: Exception, methods: str) -> None:
    # Methods only: the error text must not carry notification or server data.
    logger.warning("connector ingest notification rejected methods={} error={}", methods, error)
    rejected.append(error)


def _response_json(response: httpx.Response) -> Any:
    json_reader = getattr(response, "json", None)
    if not callable(json_reader):
        return None
    try:
        return json_reader()
    except ValueError:
        return None


def _raise_for_rejected_notifications(response: httpx.Response) -> None:
    json_reader = getattr(response, "json", None)
    if not callable(json_reader):
        return
    try:
        payload = json_reader()
    except ValueError:
        return
    if not isinstance(payload, dict):
        return
    rejected = payload.get("rejected")
    if not isinstance(rejected, list) or not rejected:
        return
    first = rejected[0] if isinstance(rejected[0], dict) else {}
    method = first.get("method") if isinstance(first.get("method"), str) else "unknown"
    code = (
        first.get("code")
        if isinstance(first.get("code"), str)
        else "notification_rejected"
    )
    message = (
        first.get("message")
        if isinstance(first.get("message"), str)
        else "backend rejected connector notification"
    )
    raise ConnectorIngestRejectedError(
        f"backend ingest rejected {len(rejected)} notification(s); "
        f"first method={method} code={code}: {message}"
    )


def coalesce_timeline_item_upserts(
    notifications: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep only the newest upsert per timeline item inside one outbound batch."""
    latest_index_by_key: dict[tuple[str, str], int] = {}
    dropped: set[int] = set()
    for index, notification in enumerate(notifications):
        if notification.get("method") != "timeline.itemUpsert":
            continue
        params = notification.get("params")
        if not isinstance(params, dict):
            continue
        session_id = params.get("sessionId")
        item = params.get("item")
        item_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(session_id, str) or not isinstance(item_id, str):
            continue
        key = (session_id, item_id)
        previous = latest_index_by_key.get(key)
        if previous is not None:
            dropped.add(previous)
        latest_index_by_key[key] = index
    if not dropped:
        return notifications
    return [
        notification
        for index, notification in enumerate(notifications)
        if index not in dropped
    ]
