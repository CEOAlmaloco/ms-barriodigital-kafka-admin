package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Pedido de aumento de particiones; solo crece, nunca disminuye. */
@Schema(description = "Nuevo total de particiones (solo puede aumentar)")
public record UpdatePartitionsRequest(
        @NotNull(message = "partitions es obligatorio")
        @Min(value = 1, message = "partitions debe ser un entero de 1 a 12")
        @Max(value = 12, message = "partitions debe ser un entero de 1 a 12")
        @Schema(example = "6")
        Integer partitions
) {
}
