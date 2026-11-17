package cl.duoc.barriodigital.kafkaadmin.adapter.in.web;

/** Resultado de un mensaje en el replay. */
public record DltReplayResultView(
        Integer partition,
        Long offset,
        String eventId,
        String status,
        String reason
) {
}
