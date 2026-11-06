package cl.duoc.barriodigital.kafkaadmin.web;

import java.util.Map;

/** Alta de un tópico del Admin (no del sistema). */
public record CreateTopicRequest(String name, Integer partitions, Integer replicationFactor,
        Map<String, String> configs) {
}
