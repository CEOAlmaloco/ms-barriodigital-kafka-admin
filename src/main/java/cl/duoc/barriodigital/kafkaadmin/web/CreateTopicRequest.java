package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Map;

/** Alta de un tópico del Admin (no del sistema). */
@Schema(description = "Alta de un topico del Admin")
public record CreateTopicRequest(
        @NotBlank(message = "name es obligatorio")
        @Pattern(regexp = "^[A-Za-z0-9._-]{1,249}$", message = "El nombre no cumple el formato")
        @Schema(example = "demo.events")
        String name,
        @NotNull(message = "partitions es obligatorio")
        @Min(value = 1, message = "partitions debe ser un entero de 1 a 12")
        @Max(value = 12, message = "partitions debe ser un entero de 1 a 12")
        @Schema(example = "3")
        Integer partitions,
        @NotNull(message = "replicationFactor es obligatorio")
        @Min(value = 1, message = "replicationFactor debe ser un entero de 1 a 3")
        @Max(value = 3, message = "replicationFactor debe ser un entero de 1 a 3")
        @Schema(example = "3")
        Integer replicationFactor,
        @Schema(example = "{\"retention.ms\":\"604800000\",\"cleanup.policy\":\"delete\"}")
        Map<String, String> configs
) {
}
