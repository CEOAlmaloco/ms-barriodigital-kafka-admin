package cl.duoc.barriodigital.kafkaadmin.web;

import java.time.Instant;

/** Un mensaje de la DLT, sin stacktrace (EP3-03, 7.1). */
public record DltMessageView(
        int partition,
        long offset,
        String key,
        Instant receivedAt,
        String eventId,
        String eventType,
        String value,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        Instant originalTimestamp,
        String consumerGroup,
        String exceptionClass,
        String exceptionCause,
        String exceptionMessage
) {
}
