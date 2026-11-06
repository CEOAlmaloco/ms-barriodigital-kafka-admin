package cl.duoc.barriodigital.kafkaadmin.web;

import java.time.Instant;

/**
 * Pedido de reset de offsets. {@code dryRun} es {@link Boolean} y no
 * primitivo: si falta, la validación responde 400 en vez de asumir false.
 */
public record ResetOffsetsRequest(String topic, String strategy, Instant timestamp, Boolean dryRun) {
}
