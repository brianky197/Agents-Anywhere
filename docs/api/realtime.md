# Realtime API

Status: proposal and current-behavior map.

Agents Anywhere uses realtime channels for three different lifecycles:

- session detail updates;
- dashboard connector/session list updates;
- connector RPC/ingest presence;
- terminal streams.

Connector channel endpoint names stay stable. Session and dashboard realtime
semantics should be tightened around the new session model:

```text
SessionMeta and SessionTimeline are durable Server facts.
Runtime state, notices, catalogs, capabilities, commands, and selections are
non-durable RuntimeLive facts.
```

## Session realtime

Primary realtime channel:

```text
WS /api/v2/sessions/{sessionId}/ws?ticket=...
```

Recovery endpoint:

```text
GET /api/v2/sessions/{sessionId}/events?after=seq:123
```

Ticket endpoint:

```text
POST /api/v2/ws-ticket
```

### Intended lifecycle

```text
GET /sessions/{sessionId}/snapshot
  -> reads durable meta/timeline from Server and RuntimeLive facts from runtime RPC
  -> receive durable eventCursor
POST /ws-ticket
  -> session scope ticket
WS /sessions/{sessionId}/ws?ticket=...
  -> receive session.subscribed
  -> receive incremental events
GET /sessions/{sessionId}/events?after=seq:...
  -> post-subscription cursor reconciliation and reconnect recovery
```

`/events` is not snapshot polling. It is a cursor recovery API. If WebSocket is healthy, the client should not call `/events` on a fixed interval.

### Target event types

```text
session.subscribed
session.meta.updated
timeline.item_created
timeline.item_updated
timeline.snapshot
runtime.state.updated
runtime.notice.snapshot
runtime.notice.updated
runtime.capability.updated
runtime.catalog.updated
runtime.refetch_required
session.refetch_required
```

Removed compatibility event types:

- `session.status_changed`
- `notice.created`
- `notice.updated`
- `notice.snapshot`
- embedded `effectiveCapabilities` aliases in session realtime events

Use `runtime.state.updated`, `runtime.capability.updated`,
`runtime.notice.updated`, and `runtime.notice.snapshot` for live Runtime facts.

### Recovery rules

- Event cursor format is `seq:{number}`.
- Timeline items are upsert-only.
- A sequence gap does not automatically require snapshot.
- Server returns `snapshotRequired=true` only when durable meta/timeline
  recovery is explicitly impossible.
- Client pulls snapshot only for initial load or `snapshotRequired=true`.
- Recovery returns the newest runtime state Server holds (the shared runtime
  state cache, else the persisted session status) as `runtime.state.updated`:
  at the current cursor always, otherwise when its `updatedSeq` is newer than
  `after`. Runtime state is only pushed live, so a transition between the
  snapshot and the WebSocket subscription is otherwise lost and the client
  keeps showing a finished turn as running. `session.meta.updated` from
  recovery carries the same status. Clients keep accepting a state only when
  its `updatedSeq` is not older than the one they hold.
- Catalogs are not recovered from Server DB. After reconnect, Web calls the
  relevant runtime live endpoint if it needs current catalogs.
- Runtime notices are recovered when Server has a sequence-backed projection.
  Session meta and the latest persisted effective capability projection are
  also returned when recovery starts at the current cursor, because presence
  and capabilities can change without advancing the durable session sequence.
  When the connector is online, Web reads the live session capability endpoint
  after recovery and keeps that Runtime result authoritative.
- `runtime.capability.updated` carries effective capabilities. On a session
  WebSocket, it is session-scoped and controls current session actions. On a
  dashboard/runtime WebSocket, it is runtime-scoped and controls setup,
  create-session, catalog, and feature entry points.
- Web should not derive action availability from `runtime.state.updated` when a
  matching effective capability exists.

## Dashboard realtime

Primary realtime channel:

```text
WS /api/v2/dashboard/ws?ticket=...
```

Removed legacy SSE:

```text
GET /api/v2/sessions/events/dashboard?token=...
```

Do not add new clients for this route. Dashboard lifecycle updates use
`/dashboard/ws`.

The Web client should prefer dashboard WebSocket and stop fixed-interval polling of:

```text
GET /api/v2/connectors
GET /api/v2/projects
GET /api/v2/sessions/list
```

### Current dashboard behavior

The current dashboard WebSocket sends:

```text
dashboard.snapshot
```

on connect, and sends another full snapshot when a debounced `dashboard.changed` invalidation arrives.

Each snapshot contains the complete owned project and session metadata inventories,
including both active and archived sessions. `sessionPages` reports no more pages.
Web and iOS group, filter and sort these sessions locally. Expanding a project,
switching a device filter or opening archives does not fetch another session list.
An explicit HTTP refresh reads `/projects` and `/sessions/list` once each, alongside
the connector list; it does not issue a request for every project or archive state.

Project mutations refresh the shared project list. A session with an unknown or
missing project binding also triggers a shared project refresh and remains visible
under ungrouped sessions. Clients coalesce concurrent project reads and remember
unresolved bindings, so every message on an unassigned session does not cause a new
request. A failed read can retry after a later update. Newer push or mutation data
takes precedence over an earlier HTTP read.

This is acceptable as a near-term replacement for polling. If full snapshots become too heavy, the next step is delta events:

```text
connector.created
connector.updated
connector.deleted
connector.presence.updated
runtime.updated
session.created
session.meta.updated
session.state.updated
session.archived
```

Do not implement delta dashboard events until the snapshot WebSocket path is stable.

## Connector realtime

Stable connector channel:

```text
WS /api/v2/connector/ws
```

Stable connector HTTP endpoints:

```text
POST /api/v2/connector/auth
POST /api/v2/connector/ingest
POST /api/v2/connector/timeline/manifest
```

These endpoint names should not change as part of the runtime protocol refactor.

### Connector runtime RPC methods

The runtime protocol refactor evolves payload semantics behind
`WS /api/v2/connector/ws`. Runtime configuration is provider-managed and
read-only once a runtime instance is running:

```text
runtime.discover
runtime.configSchema
runtime.config
runtime.validateConfig
runtime.start
runtime.stop
runtime.modelCatalog
runtime.permissionCatalog
session.discover
session.create
session.sync
session.state
session.selections.update
session.commands
session.command.execute
interaction.respond
turn.start
turn.steer
turn.interrupt
```

`runtime.configSchema` performs a live provider read for UI/CLI configuration
forms. `runtime.config` returns saved raw values when the runtime is stopped,
and the running runtime's read-only effective `RuntimeConfig` projection when
it is running. Config mutation still flows through `runtime.validateConfig` and
`runtime.start`; running `AgentRuntime` instances do not accept direct config
updates.

`runtime.discover` includes runtime-native capability inputs. They are not the
primary frontend contract. The frontend contract is effective capability:

```text
GET /connectors/{connectorId}/runtimes/{runtime}/capabilities
GET /sessions/{sessionId}/runtime/capabilities
runtime.capability.updated
```

The Server may apply authorization, takeover, connector reachability, and
feature policy. It must not infer runtime-owned action availability from
durable session data.

Runtime host live events are sent as connector WebSocket notifications on
`WS /api/v2/connector/ws`. `POST /api/v2/connector/ingest` is reserved for
explicit bulk sync and disconnected WebSocket fallback; it must still compute
changed timeline items and publish session WebSocket events for frontend
convergence.

`POST /api/v2/connector/ingest` isolates notification failures inside a batch.
One malformed or conflicting notification must not produce HTTP 500 or prevent
later notifications in the same request from being applied. The response
includes:

```json
{
  "accepted": 2,
  "rejected": [
    {
      "index": 1,
      "method": "timeline.itemUpsert",
      "code": "notification_failed",
      "message": "...",
      "errorType": "ValidationError"
    }
  ],
  "features": ["timeline.reconcile"],
  "serverTime": "..."
}
```

`accepted` counts successfully applied notifications. `rejected` reports
per-notification failures using the original zero-based batch index. Explicit
protocol violations that are intentionally unsupported may still return HTTP
400; unexpected processing failures should be logged and reported through
`rejected` instead of surfacing as HTTP 500.

### Reconciling complete snapshots

A `timeline.sync` with `complete: true` replaces the session's whole timeline,
so it has to travel as one request, and a large session exceeds the ingress
body limit (HTTP 413, issue #278). Splitting it does not help: a later complete
page would delete the earlier ones. Instead, the Connector compares the
snapshot with what the Server stores and sends appends and deletes, each of
which is an ordinary bounded request.

```text
POST /api/v2/connector/timeline/manifest
{ "sessionId", "externalSessionId"?, "runtime", "runtimeId", "after"?, "limit"? }
-> { "items": [{ "id", "orderSeq", "contentHash" }], "next": { "orderSeq", "id" } | null }
```

- The manifest pages the stored items in `(orderSeq, id)` order, up to `limit`
  (default 1000, at most 5000) per page. Pass `next` back as `after` until it is
  null. The session is resolved from `sessionId` and `externalSessionId`, as a
  reconcile write naming the same pair resolves it. A session the Server does
  not have yet has an empty manifest. Items accepted but still buffered for the
  session are stored before the page is read, as a replacing snapshot would
  store them, so they can be deleted too. It requires the Connector access
  token; an unconfigured runtime answers 409 `runtime_not_configured`.
- `timeline.sync` with `complete: false` and `orderAuthoritative: true` stores
  each item's `orderSeq` as given, also for existing items. An item whose order
  or `contentHash` differs is written; others are unchanged. When the request
  has an `externalSessionId` key (a null value included), the session is
  resolved from it, like the manifest and `timeline.delete`, instead of from the
  first item's `source.sessionId`, so every page of changed items lands in the
  same session. Moving an existing item, or inserting one at or below the
  current last `orderSeq`, is a reset: session subscribers get
  `session.refetch_required`, and event recovery from before it reports
  `snapshotRequired`. Appending after the end publishes ordinary item events.
- `timeline.delete { sessionId, runtime, runtimeId, externalSessionId?, itemIds }`
  deletes up to 10000 item IDs. Unknown IDs are ignored, so a repeated delete
  changes nothing. A delete that removes anything is a reset, like above. A
  malformed `itemIds` list returns HTTP 400 `invalid_timeline_delete`.
- Every ingest response of a Server that applies these semantics lists
  `"timeline.reconcile"` in `features`. An older Server accepts the unknown
  notification and parameter as no-ops, so a response without it means the
  writes were not applied, for example while a deploy is rolling.

For each complete snapshot, in order with the rest of the batch, the Connector:

1. reads the manifest once the notifications before the snapshot have been
   applied, naming the session by the snapshot's first `source.sessionId` (the
   ID the Server would resolve the complete snapshot by);
2. sends one incremental `timeline.sync` with `orderAuthoritative` and that
   `externalSessionId`, holding only the items whose `(orderSeq, contentHash)`
   differ from the manifest (a repeated ID keeps its last value), paged like any
   other incremental sync; it is sent even when empty, since it carries source
   observation time;
3. only if every page was accepted, sends `timeline.delete` for the manifest IDs
   the snapshot no longer has, naming the same session.

It deletes only IDs it read from the manifest, so items stored after the read
are kept. The end state is the same as a replacing snapshot, but unchanged
history is never uploaded again and no request has to hold the whole session.
A refused change leaves the session partly updated and is reported like any
refused notification; nothing the snapshot still has is deleted. The rewritten
notifications replace the snapshot in the pending batch, so a transient failure
resumes at the first unaccepted page, and a change refused before the failure
still holds back the deletes.

A snapshot is reconciled only when all of its items name the same native
session in `source.sessionId`, or none of them does; otherwise it is sent
whole. A Server too old for `externalSessionId` resolution resolves each page
from that page's own items, so a page of only some of them could land in
another session.

A Server without the manifest answers 404. The Connector then sends complete
snapshots whole, as before, and asks again after 10 minutes. A reconcile write
that is accepted but answered without `"timeline.reconcile"` does the same: no
further page of that snapshot is sent, and the snapshot is sent whole right away.
A refused reconcile write is reported as refused, and its deletes are not sent. Other manifest
refusals send that snapshot whole; 408, 429 and 5xx are retried later. Old Connectors keep sending complete snapshots,
which the Server still applies as replacements. Items with equal `orderSeq`
have no defined relative order in either path; Runtimes give each item its own.

The DSH relay reads the manifest within the `snapshot.begin` acknowledgement,
naming the capture's native session. The notifications the Connector already
queued are delivered first, under the ingest lock; the manifest pages are then
read without it, so other sessions keep sending. It uploads each page's changed
items before acknowledging that page, and sends the deletes with the commit.
A page with an item that does not name the capture's native session, or any
refusal, sends the capture as one complete snapshot at the commit instead. A
capture once refused as too large is reconciled again when a manifest is
available, rather than skipped.

The bridge times out a whole transport batch after 60 seconds. When
`snapshot.begin` has a batch of its own, that time covers waiting for the
ingest lock, delivering the queued notifications and reading every manifest
page; each later page's batch covers uploading that page's changes. When the
bridge buffers live changes, `snapshot.begin`, pages and even the commit can
share one batch, and so one timeout. There is no overall budget for the
manifest read; a manifest that takes longer than one batch timeout fails the
feed, which subscribes again. Splitting acknowledgements per operation would
not remove that limit on its own.

The target semantic connector notification methods are:

```text
session.meta.upsert
timeline.sync
timeline.itemUpsert
timeline.delete
runtime.state.updated
runtime.notice.snapshot
runtime.notice.updated
runtime.capability.updated
runtime.catalog.updated
runtime.error
```

Compatibility connector notification names may remain during migration:

```text
session.state.updated
notice.upsert
protocol.capabilitiesUpdated
```

The Server should translate compatibility names into RuntimeLive events without
treating them as durable session truth.

The connector application layer bridges `RuntimeHostClient` calls to these
server-facing notification payloads. Runtime adapters should never call
connector HTTP/WS transports directly.

## Terminal realtime

Current terminal channels include:

```text
WS /api/v2/sessions/{sessionId}/terminals/{terminalId}/stream
WS /api/v2/connectors/{connectorId}/terminals/{terminalId}/stream
WS /api/v2/connectors/{connectorId}/terminals-v2/{terminalId}/stream
WS /api/v2/connector/terminals/{terminalId}/relay
```

Terminal APIs are local capability APIs, not Agent Runtime Protocol session APIs. They can be cleaned up later as a separate local-capabilities interface pass.

Do not block the runtime protocol migration on terminal endpoint consolidation.
