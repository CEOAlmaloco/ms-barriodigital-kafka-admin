package cl.duoc.barriodigital.kafkaadmin.messaging;

/** Payload de un evento administrativo en audit.timeline. */
public record AuditPayload(
        String resourceType,
        String resourceId,
        String action,
        Object before,
        Object after
) {
}
