package cl.duoc.barriodigital.kafkaadmin.adapter.in.web;

import java.util.List;

/** Respuesta de GET /api/kafka/dlt/{name}/messages. */
public record DltMessagesView(
        String dlt,
        List<DltPartitionView> partitions
) {
}
