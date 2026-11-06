package cl.duoc.barriodigital.kafkaadmin.web;

import java.util.Map;

/** Cambio parcial de configuraciones; los valores viajan como string. */
public record UpdateTopicConfigsRequest(Map<String, String> configs) {
}
