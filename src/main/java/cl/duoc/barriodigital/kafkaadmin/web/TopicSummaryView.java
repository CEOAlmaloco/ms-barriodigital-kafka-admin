package cl.duoc.barriodigital.kafkaadmin.web;

/** Un tópico en el listado de /api/kafka/topics. */
public record TopicSummaryView(String name, int partitions, int replicationFactor, boolean system) {
}
