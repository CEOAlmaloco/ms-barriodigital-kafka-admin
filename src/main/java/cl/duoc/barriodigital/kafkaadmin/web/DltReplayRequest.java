package cl.duoc.barriodigital.kafkaadmin.web;

import java.util.List;

/** Cuerpo de POST /api/kafka/dlt/{name}/replay: offsets o eventIds, nunca ambos. */
public record DltReplayRequest(
        List<OffsetRef> offsets,
        List<String> eventIds
) {
    public record OffsetRef(Integer partition, Long offset) {
    }
}
