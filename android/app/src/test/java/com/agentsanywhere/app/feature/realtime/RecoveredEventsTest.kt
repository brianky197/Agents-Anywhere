package com.agentsanywhere.app.feature.realtime

import com.agentsanywhere.app.api.RemoteSessionEventEnvelope
import com.agentsanywhere.app.api.RemoteSessionEventPayload
import org.junit.Assert.assertEquals
import org.junit.Test

class RecoveredEventsTest {
    private fun event(type: String, sequence: Long, eventId: String) = RemoteSessionEventEnvelope(
        protocolVersion = "1.0",
        eventId = eventId,
        sequence = sequence,
        cursor = "seq:$sequence",
        type = type,
        sessionId = "session",
        emittedAt = null,
        payload = RemoteSessionEventPayload(),
    )

    @Test
    fun recoveryLeavesRuntimeStateToTheRuntimeRefresh() {
        val recovered = recoveredEventsToApply(
            listOf(
                event("runtime.state.updated", 12, "state"),
                event("runtime.capability.updated", 12, "capabilities"),
                event("session.meta.updated", 12, "meta"),
                event("timeline.item_created", 11, "item"),
            ),
        )

        assertEquals(
            listOf("item", "capabilities", "meta"),
            recovered.map(RemoteSessionEventEnvelope::eventId),
        )
    }
}
