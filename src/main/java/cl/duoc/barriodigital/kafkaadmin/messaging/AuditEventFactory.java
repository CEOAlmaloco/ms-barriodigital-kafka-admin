package cl.duoc.barriodigital.kafkaadmin.messaging;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Arma el sobre de auditoría con eventId, traceId y occurredAt. */
@Component
public class AuditEventFactory {

    public static final String PRODUCER = "kafka-admin";
    public static final int SCHEMA_VERSION = 1;

    private final Clock clock;

    public AuditEventFactory(Clock clock) {
        this.clock = clock;
    }

    public AuditEnvelope create(String resourceType, String resourceId, String action, Object before, Object after) {
        AuditActor actor = ActorContext.get();
        if (actor == null) {
            actor = new AuditActor("unknown", "Sin nombre", "Admin", null);
        }
        AuditPayload payload = new AuditPayload(resourceType, resourceId, action, before, after);
        return new AuditEnvelope(
                UUID.randomUUID(),
                resourceType + "." + action,
                SCHEMA_VERSION,
                Instant.now(clock),
                PRODUCER,
                UUID.randomUUID(),
                actor,
                payload
        );
    }
}
