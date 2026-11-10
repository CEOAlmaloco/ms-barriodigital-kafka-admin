package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

/**
 * Pedido de reset de offsets. {@code dryRun} es {@link Boolean} con {@code @NotNull},
 * no primitivo: si falta, responde 400 en vez de asumir false.
 */
@Schema(description = "Reset de offsets de un consumer group")
public record ResetOffsetsRequest(
        @NotBlank(message = "topic es obligatorio")
        @Schema(example = "requests.events")
        String topic,
        @Schema(example = "EARLIEST")
        String strategy,
        @Schema(example = "2026-11-10T00:00:00Z")
        Instant timestamp,
        @NotNull(message = "dryRun es obligatorio")
        @Schema(example = "true")
        Boolean dryRun
) {
}
