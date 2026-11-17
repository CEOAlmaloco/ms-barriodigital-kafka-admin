package cl.duoc.barriodigital.kafkaadmin.adapter.in.web.dto;

import java.util.List;

/** Respuesta 200 del replay, aunque haya fallidos. */
public record DltReplayResponse(
        String dlt,
        String targetTopic,
        int resent,
        int skipped,
        int failed,
        int notAttempted,
        List<DltReplayResultView> results
) {
}
