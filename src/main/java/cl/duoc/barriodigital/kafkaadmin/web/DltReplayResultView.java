package cl.duoc.barriodigital.kafkaadmin.web;

/** Resultado de un mensaje en el replay. */
public record DltReplayResultView(
        Integer partition,
        Long offset,
        String eventId,
        String status,
        String reason
) {
}
