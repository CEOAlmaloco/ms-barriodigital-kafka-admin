package cl.duoc.barriodigital.kafkaadmin.messaging;

/** Actor del sobre de audit.timeline (EP3-01, 4 y 9). */
public record AuditActor(String id, String name, String role, String sourceIp) {
}
