package cl.duoc.barriodigital.kafkaadmin.messaging;

import java.time.Instant;
import java.util.UUID;

/** Sobre de audit.timeline (sin sequence; con actor.sourceIp). */
public record AuditEnvelope(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String producer,
        UUID traceId,
        AuditActor actor,
        AuditPayload payload
) {
    public String key() {
        return payload.resourceType() + ":" + payload.resourceId();
    }
}
