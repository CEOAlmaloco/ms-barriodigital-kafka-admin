package cl.duoc.barriodigital.kafkaadmin.service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/**
 * Lectura y publicación de registros fuera de {@code AdminClient}.
 * El listado de DLT asigna particiones sin {@code group.id}; el replay espera confirmación.
 */
public interface KafkaRecordClient {

    /** Registro leído de una partición asignada. */
    record ConsumedRecord(
            int partition,
            long offset,
            String key,
            String value,
            Instant timestamp,
            Headers headers
    ) {
    }

    /**
     * Lee hasta {@code limitPerPartition} mensajes por partición, del {@code fromOffset}
     * (incluido) hacia adelante, con tope de tiempo. Sin {@code group.id} ni commit.
     */
    List<ConsumedRecord> readAssigned(
            String topic,
            Map<Integer, Long> fromOffsetsByPartition,
            int limitPerPartition,
            Duration timeout
    );

    /**
     * Publica un mensaje a una partición concreta y espera la confirmación del broker.
     * @throws java.util.concurrent.TimeoutException si no confirma a tiempo
     * @throws Exception si el envío falla
     */
    void sendConfirmed(
            String topic,
            int partition,
            String key,
            String value,
            Instant recordTimestamp,
            Headers headers,
            Duration wait
    ) throws Exception;
}
