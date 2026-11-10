package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import java.util.List;

/** Cuerpo de POST /api/kafka/dlt/{name}/replay: offsets o eventIds, nunca ambos. */
@Schema(description = "Replay selectivo: offsets o eventIds, uno de los dos")
public record DltReplayRequest(
        @Valid
        @Size(max = 100, message = "El replay admite hasta 100 mensajes")
        @Schema(example = "[{\"partition\":1,\"offset\":2040}]")
        List<OffsetRef> offsets,
        @Size(max = 100, message = "El replay admite hasta 100 mensajes")
        @Schema(example = "[\"9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d\"]")
        List<String> eventIds
) {
    public record OffsetRef(
            @Schema(example = "1") Integer partition,
            @Schema(example = "2040") Long offset
    ) {
    }
}
