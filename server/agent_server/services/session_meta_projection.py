from __future__ import annotations

from agent_server.core.models import SessionRuntimeState, SessionView
from agent_server.services.connector_presence import ConnectorPresencePort
from agent_server.services.repository_ports import SessionRuntimeStateRepository
from agent_server.services.session_runtime_state_cache import SessionRuntimeStateCache


def session_with_runtime_state(
    session: SessionView,
    state: SessionRuntimeState,
) -> SessionView:
    return session.model_copy(
        update={
            "externalSessionId": state.externalSessionId or session.externalSessionId,
            "status": state.status,
        }
    )


async def read_runtime_state_snapshot(
    store: SessionRuntimeStateRepository,
    runtime_state_cache: SessionRuntimeStateCache,
    session: SessionView,
    user_id: str | None,
) -> SessionRuntimeState:
    """Read the runtime state a snapshot can serve without connector RPC.

    A live read is runtime-owned work that can take seconds on a large history
    (for example a DSH session log replay). Snapshot hydration therefore returns
    the newest cached or persisted fact and refreshes the live state in the
    background, where a real change is pushed to subscribers.
    """

    cached_state = await runtime_state_cache.get(session.id)
    if cached_state is not None:
        return cached_state
    return await store.get_session_runtime_state(session.id, user_id=user_id)


async def project_session_meta_for_dashboard(
    presence: ConnectorPresencePort,
    runtime_state_cache: SessionRuntimeStateCache,
    sessions: list[SessionView],
) -> list[SessionView]:
    connector_statuses = await connector_online_statuses(presence, sessions)
    runtime_states = await runtime_state_cache.get_many([session.id for session in sessions])
    projected_sessions: list[SessionView] = []
    for session in sessions:
        connector_status = (
            "online" if connector_statuses.get(session.connectorId, False) else "offline"
        )
        projected = session.model_copy(update={"connectorStatus": connector_status})
        runtime_state = runtime_states.get(session.id)
        if runtime_state is not None:
            projected = session_with_runtime_state(projected, runtime_state)
        projected_sessions.append(projected)
    return projected_sessions


async def connector_online_statuses(
    presence: ConnectorPresencePort,
    sessions: list[SessionView],
) -> dict[str, bool]:
    connector_ids = [session.connectorId for session in sessions]
    batch = getattr(presence, "online_statuses", None)
    if callable(batch):
        return await batch(connector_ids)
    unique_ids = list(dict.fromkeys(connector_ids))
    return {
        connector_id: await presence.is_online(connector_id)
        for connector_id in unique_ids
    }
