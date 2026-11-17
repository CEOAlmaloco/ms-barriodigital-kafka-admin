package cl.duoc.barriodigital.kafkaadmin.adapter.in.web.dto;

/** Un tópico en el listado de /api/kafka/topics. */
public record TopicSummaryView(String name, int partitions, int replicationFactor, boolean system) {
}
