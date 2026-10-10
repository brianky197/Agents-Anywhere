"""Reconcile a complete timeline snapshot as appends and deletes (issue #278).

A complete ``timeline.sync`` replaces the whole session timeline, so it used to
travel as one request; a large session exceeded the ingress body limit. With a
Server that serves the timeline manifest, the Connector instead compares the
snapshot with what is stored and sends:

- one incremental ``timeline.sync`` with ``orderAuthoritative`` holding only the
  items whose order or content hash differ; it is paged like any other
  incremental sync, and
- ``timeline.delete`` for stored IDs the snapshot no longer has.

Only IDs read from the manifest are deleted, so items the Server received after
the manifest was read are kept. The result matches a replacing snapshot.
"""

from __future__ import annotations

from typing import Any

# The Server stores ``(orderSeq, contentHash)`` per item ID.
Manifest = dict[str, tuple[int, str]]

MANIFEST_PAGE_ITEMS = 2000
DELETE_CHUNK_ITEMS = 5000
_SNAPSHOT_ONLY_PARAMS = frozenset({"items", "complete"})
_IDENTITY_PARAMS = ("sessionId", "runtime", "runtimeId", "metadata")


def is_full_snapshot(notification: dict[str, Any]) -> bool:
    params = notification.get("params")
    return (
        notification.get("method") == "timeline.sync"
        and isinstance(params, dict)
        and params.get("complete") is True
        and isinstance(params.get("items"), list)
        and isinstance(params.get("sessionId"), str)
    )


def external_session_id(items: list[Any]) -> str | None:
    """The native session ID the Server resolves a timeline.sync by.

    The Server takes it from the first item that names one, and ignores the
    sync's own ``externalSessionId``; the manifest and the delete pass this one,
    so all three resolve the same session.
    """
    for item in items:
        source = item.get("source") if isinstance(item, dict) else None
        value = source.get("sessionId") if isinstance(source, dict) else None
        if isinstance(value, str) and value:
            return value
    return None


def manifest_request(params: dict[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {
        key: params[key] for key in ("sessionId", "runtime", "runtimeId") if isinstance(params.get(key), str)
    }
    external = external_session_id(params.get("items") or [])
    if external is not None:
        body["externalSessionId"] = external
    return body


def changed_items(items: list[Any], manifest: Manifest) -> list[Any]:
    """Items whose order or content differ from the stored ones."""
    latest: dict[str, Any] = {}
    for item in items:
        latest[item["id"]] = item
    return [
        item for item in latest.values()
        if manifest.get(item["id"]) != (item.get("orderSeq"), item.get("contentHash"))
    ]


def delete_notifications(
    params: dict[str, Any], external_session_id: str | None, item_ids: list[str]
) -> list[dict[str, Any]]:
    identity = {key: params[key] for key in _IDENTITY_PARAMS if key in params}
    if external_session_id is not None:
        identity["externalSessionId"] = external_session_id
    return [
        {"method": "timeline.delete", "params": {**identity, "itemIds": item_ids[start:start + DELETE_CHUNK_ITEMS]}}
        for start in range(0, len(item_ids), DELETE_CHUNK_ITEMS)
    ]


def reconcile_snapshot(notification: dict[str, Any], manifest: Manifest) -> list[dict[str, Any]]:
    """Rewrite one complete ``timeline.sync`` against the stored manifest."""
    params = notification["params"]
    items = params["items"]
    present = {item["id"] for item in items}
    metadata = {key: value for key, value in params.items() if key not in _SNAPSHOT_ONLY_PARAMS}
    external = external_session_id(items)
    changed = changed_items(items, manifest)
    if not changed and items:
        # Always sent, since it carries source observation time. One unchanged
        # item is a no-op on the Server but resolves the same session.
        changed = [next((item for item in items if external_session_id([item]) == external), items[0])]
    upserts = {"method": "timeline.sync", "params": {
        **metadata, "complete": False, "orderAuthoritative": True, "items": changed}}
    removed = [item_id for item_id in manifest if item_id not in present]
    return [upserts, *delete_notifications(params, external, removed)]
