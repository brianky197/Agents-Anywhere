"""Reconcile a complete timeline snapshot as appends and deletes (issue #278).

A complete ``timeline.sync`` replaces the whole session timeline, so it used to
travel as one request; a large session exceeded the ingress body limit. With a
Server that serves the timeline manifest, the Connector instead compares the
snapshot with what is stored and sends, as one step:

- one incremental ``timeline.sync`` with ``orderAuthoritative`` holding only the
  items whose order or content hash differ; it is paged like any other
  incremental sync, and then
- ``timeline.delete`` for stored IDs the snapshot no longer has, only if every
  change was accepted.

Only IDs read from the manifest are deleted, so items the Server received after
the manifest was read are kept. The result matches a replacing snapshot.
"""

from __future__ import annotations

from typing import Any

from connector.server.ingest_batching import RECONCILE_KEY

# The Server stores ``(orderSeq, contentHash)`` per item ID.
Manifest = dict[str, tuple[int, str]]

MANIFEST_PAGE_ITEMS = 5000
DELETE_CHUNK_ITEMS = 5000
# Ingest responses of a Server that applies reconcile writes carry this.
RECONCILE_FEATURE = "timeline.reconcile"
_SNAPSHOT_ONLY_PARAMS = frozenset({"items", "complete"})
_IDENTITY_PARAMS = ("sessionId", "runtime", "runtimeId", "metadata")


class ReconcileStep:
    """Ties the notifications of one reconciled snapshot together.

    It travels under ``RECONCILE_KEY`` in each notification dict and is never
    sent. It keeps the original snapshot, so an older Server that accepted the
    writes without applying them still gets the snapshot whole.
    """

    __slots__ = ("failed", "original", "unconfirmed")

    def __init__(self, original: dict[str, Any]) -> None:
        self.original = original
        # A change was refused: its deletes must not follow.
        self.failed = False
        # A response lacked RECONCILE_FEATURE: replace with the original.
        self.unconfirmed = False


def is_full_snapshot(notification: dict[str, Any]) -> bool:
    params = notification.get("params")
    return (
        notification.get("method") == "timeline.sync"
        and isinstance(params, dict)
        and params.get("complete") is True
        and isinstance(params.get("items"), list)
        and isinstance(params.get("sessionId"), str)
    )


def has_reconcile_semantics(notification: dict[str, Any]) -> bool:
    """Writes an older Server would accept without applying them."""
    params = notification.get("params")
    return notification.get("method") == "timeline.delete" or (
        notification.get("method") == "timeline.sync"
        and isinstance(params, dict)
        and params.get("orderAuthoritative") is True
    )


def external_session_id(items: list[Any]) -> str | None:
    """The native session ID the Server resolves a complete snapshot by.

    The Server takes it from the first item that names one. Reconcile writes
    then name it explicitly, so every page and delete resolves that session.
    """
    for item in items:
        source = item.get("source") if isinstance(item, dict) else None
        value = source.get("sessionId") if isinstance(source, dict) else None
        if isinstance(value, str) and value:
            return value
    return None


def item_session_id(item: Any) -> str | None:
    """The native session an item names, as the Server reads it."""
    source = item.get("source") if isinstance(item, dict) else None
    value = source.get("sessionId") if isinstance(source, dict) else None
    return value if isinstance(value, str) and value else None


def names_one_session(items: list[Any]) -> bool:
    """Whether every item names the same native session, or none does.

    A Server too old to resolve reconcile writes by ``externalSessionId`` takes
    a page's session from its own items. When all items agree, any page of
    changes resolves the session the complete snapshot would, so such a Server
    can at worst ignore the new semantics, which the Connector detects and
    replaces; it cannot write into another session.
    """
    return len({item_session_id(item) for item in items}) <= 1


def manifest_request(params: dict[str, Any]) -> dict[str, Any]:
    """Name a complete snapshot's session for the manifest."""
    body: dict[str, Any] = {
        key: params[key] for key in ("sessionId", "runtime", "runtimeId") if isinstance(params.get(key), str)
    }
    external = external_session_id(params.get("items") or [])
    if external is not None:
        body["externalSessionId"] = external
    return body


def changed_items(items: list[Any], manifest: Manifest) -> list[Any]:
    """Items whose order or content differ from the stored ones.

    A repeated ID keeps its last value, as the Server does.
    """
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
    external = external_session_id(items)
    metadata = {key: value for key, value in params.items() if key not in _SNAPSHOT_ONLY_PARAMS}
    step = ReconcileStep(notification)
    # Sent even when nothing changed: it still carries source observation time.
    upserts = {"method": "timeline.sync", RECONCILE_KEY: step, "params": {
        **metadata, "complete": False, "orderAuthoritative": True, "externalSessionId": external,
        "items": changed_items(items, manifest)}}
    removed = [item_id for item_id in manifest if item_id not in present]
    deletes = [{**delete, RECONCILE_KEY: step} for delete in delete_notifications(params, external, removed)]
    return [upserts, *deletes]
