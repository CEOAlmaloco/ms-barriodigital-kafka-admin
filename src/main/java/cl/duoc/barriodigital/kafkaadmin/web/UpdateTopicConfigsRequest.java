package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

/** Cambio parcial de configuraciones; los valores viajan como string. */
@Schema(description = "Cambio parcial de configuraciones editables")
public record UpdateTopicConfigsRequest(
        @NotNull(message = "configs es obligatorio")
        @Schema(example = "{\"retention.ms\":\"259200000\"}")
        Map<String, String> configs
) {
}
