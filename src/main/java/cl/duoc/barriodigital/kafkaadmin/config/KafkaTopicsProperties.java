package cl.duoc.barriodigital.kafkaadmin.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Tópicos del sistema declarados en {@code barriodigital.kafka.topics}.
 * Los nombres son las claves del mapa (por ejemplo {@code requests.events})
 * y cada valor lleva particiones, réplicas y configuraciones del contrato K3.
 */
@Component
@ConfigurationProperties(prefix = "barriodigital.kafka")
public class KafkaTopicsProperties {

    private Map<String, TopicSpec> topics = new LinkedHashMap<>();

    /** Tópicos declarados en el yml, por nombre. */
    public Map<String, TopicSpec> getTopics() {
        return topics;
    }

    /** Spring asigna el mapa leído del yml. */
    public void setTopics(Map<String, TopicSpec> topics) {
        this.topics = topics;
    }

    /** Parámetros de un tópico: particiones, réplicas y configs de Kafka. */
    public static class TopicSpec {

        private Integer partitions;
        private Integer replicationFactor;
        private String cleanupPolicy;
        private Long retentionMs;
        private Integer minInsyncReplicas;
        private Long minCompactionLagMs;
        private String messageTimestampType;

        /** Cantidad de particiones del tópico. */
        public Integer getPartitions() {
            return partitions;
        }

        /** Spring asigna las particiones leídas del yml. */
        public void setPartitions(Integer partitions) {
            this.partitions = partitions;
        }

        /** Factor de replicación del tópico. */
        public Integer getReplicationFactor() {
            return replicationFactor;
        }

        /** Spring asigna el factor de replicación leído del yml. */
        public void setReplicationFactor(Integer replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        /** Política de limpieza: delete, compact o compact,delete. */
        public String getCleanupPolicy() {
            return cleanupPolicy;
        }

        /** Spring asigna la política de limpieza leída del yml. */
        public void setCleanupPolicy(String cleanupPolicy) {
            this.cleanupPolicy = cleanupPolicy;
        }

        /** Retención en milisegundos. */
        public Long getRetentionMs() {
            return retentionMs;
        }

        /** Spring asigna la retención leída del yml. */
        public void setRetentionMs(Long retentionMs) {
            this.retentionMs = retentionMs;
        }

        /** Réplicas mínimas en sincronía para aceptar escrituras. */
        public Integer getMinInsyncReplicas() {
            return minInsyncReplicas;
        }

        /** Spring asigna min.insync.replicas leído del yml. */
        public void setMinInsyncReplicas(Integer minInsyncReplicas) {
            this.minInsyncReplicas = minInsyncReplicas;
        }

        /** Antigüedad mínima antes de compactar, en milisegundos. */
        public Long getMinCompactionLagMs() {
            return minCompactionLagMs;
        }

        /** Spring asigna min.compaction.lag.ms leído del yml. */
        public void setMinCompactionLagMs(Long minCompactionLagMs) {
            this.minCompactionLagMs = minCompactionLagMs;
        }

        /** Tipo de timestamp: CreateTime o LogAppendTime (DLT). */
        public String getMessageTimestampType() {
            return messageTimestampType;
        }

        /** Spring asigna message.timestamp.type leído del yml. */
        public void setMessageTimestampType(String messageTimestampType) {
            this.messageTimestampType = messageTimestampType;
        }

        /** Configuraciones de Kafka del tópico, en el formato del broker. */
        public Map<String, String> toConfigs() {
            Map<String, String> configs = new LinkedHashMap<>();
            if (cleanupPolicy != null) {
                configs.put("cleanup.policy", cleanupPolicy);
            }
            if (retentionMs != null) {
                configs.put("retention.ms", String.valueOf(retentionMs));
            }
            if (minInsyncReplicas != null) {
                configs.put("min.insync.replicas", String.valueOf(minInsyncReplicas));
            }
            if (minCompactionLagMs != null) {
                configs.put("min.compaction.lag.ms", String.valueOf(minCompactionLagMs));
            }
            if (messageTimestampType != null) {
                configs.put("message.timestamp.type", messageTimestampType);
            }
            return configs;
        }
    }
}
