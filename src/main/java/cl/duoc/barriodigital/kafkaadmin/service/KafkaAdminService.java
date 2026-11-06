package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties.TopicSpec;
import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Único punto del servicio que habla con {@link AdminClient}.
 * Los controladores nunca usan clases de Kafka: piden métodos de alto nivel.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration PING_TIMEOUT = Duration.ofSeconds(3);

    private final AdminClient admin;
    private final KafkaTopicsProperties topicsProperties;

    public KafkaAdminService(AdminClient admin, KafkaTopicsProperties topicsProperties) {
        this.admin = admin;
        this.topicsProperties = topicsProperties;
    }

    /**
     * Kafka responde si el cluster contesta un describeCluster a tiempo.
     * Los endpoints que dependen de Kafka usan este chequeo para el 503.
     */
    public boolean isAvailable() {
        try {
            admin.describeCluster().clusterId().get(PING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Crea los tópicos del sistema que falten y avisa por WARN si alguno
     * existente difiere de la configuración declarada, sin modificarlo.
     */
    public void ensureSystemTopics() {
        if (!isAvailable()) {
            throw new KafkaUnavailableException("Kafka no esta disponible");
        }
        Set<String> existing = listTopicNames();
        List<NewTopic> missing = new ArrayList<>();
        for (Map.Entry<String, TopicSpec> entry : topicsProperties.getTopics().entrySet()) {
            String name = entry.getKey();
            TopicSpec spec = entry.getValue();
            if (existing.contains(name)) {
                warnOnConfigDifferences(name, spec);
            } else {
                missing.add(toNewTopic(name, spec));
            }
        }
        if (!missing.isEmpty()) {
            try {
                CreateTopicsResult result = admin.createTopics(missing, new CreateTopicsOptions());
                result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaUnavailableException("Kafka no esta disponible", e);
            } catch (Exception e) {
                throw new KafkaUnavailableException("Kafka no esta disponible", e);
            }
            log.info("Topicos del sistema creados: {}", missing.stream().map(NewTopic::name).toList());
        } else {
            log.info("Los {} topicos del sistema ya existen", topicsProperties.getTopics().size());
        }
    }

    /** Nombres de todos los tópicos, sin los internos (__consumer_offsets, etc.). */
    public Set<String> listTopicNames() {
        try {
            return admin.listTopics(new ListTopicsOptions().listInternal(false))
                    .names()
                    .get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new KafkaUnavailableException("Kafka no esta disponible", e);
        }
    }

    /** Descripción neutral de un consumer group: estado y cantidad de miembros. */
    public record GroupDescription(String state, int members) {
    }

    /** IDs de los consumer groups que conoce el broker. */
    public Set<String> listGroupIds() {
        try {
            ListConsumerGroupsResult result = admin.listConsumerGroups();
            java.util.Collection<ConsumerGroupListing> listings =
                    result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Set<String> ids = new java.util.HashSet<>();
            listings.forEach(l -> ids.add(l.groupId()));
            return ids;
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    /** Estado y miembros de cada grupo pedido. Los que no existen no aparecen en el mapa. */
    public Map<String, GroupDescription> describeGroups(Collection<String> groupIds) {
        if (groupIds.isEmpty()) {
            return Map.of();
        }
        try {
            DescribeConsumerGroupsResult result = admin.describeConsumerGroups(groupIds);
            Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> described =
                    result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<String, GroupDescription> out = new java.util.HashMap<>();
            described.forEach((id, d) -> out.put(id, new GroupDescription(
                    d.state() == null ? null : d.state().name(), d.members().size())));
            return out;
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    /** Offsets confirmados del grupo, por tópico y partición. */
    public Map<TopicPartition, Long> committedOffsets(String groupId) {
        try {
            ListConsumerGroupOffsetsResult result = admin.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    result.partitionsToOffsetAndMetadata().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> out = new java.util.HashMap<>();
            offsets.forEach((tp, o) -> out.put(tp, o.offset()));
            return out;
        } catch (GroupIdNotFoundException e) {
            return Map.of();
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    /** Primer offset disponible de cada partición pedida. */
    public Map<TopicPartition, Long> earliestOffsets(Collection<TopicPartition> partitions) {
        return offsets(partitions, tp -> OffsetSpec.earliest());
    }

    /** Offset final (latest) de cada partición pedida. */
    public Map<TopicPartition, Long> latestOffsets(Collection<TopicPartition> partitions) {
        return offsets(partitions, tp -> OffsetSpec.latest());
    }

    /** Primer offset con timestamp mayor o igual al instante pedido, por partición. */
    public Map<TopicPartition, Long> offsetsAtTimestamp(Collection<TopicPartition> partitions, java.time.Instant timestamp) {
        long epochMs = timestamp.toEpochMilli();
        return offsets(partitions, tp -> OffsetSpec.forTimestamp(epochMs));
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, java.util.function.Function<TopicPartition, OffsetSpec> spec) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        try {
            Map<TopicPartition, OffsetSpec> requests = new java.util.HashMap<>();
            partitions.forEach(tp -> requests.put(tp, spec.apply(tp)));
            ListOffsetsResult result = admin.listOffsets(requests);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                    result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> out = new java.util.HashMap<>();
            infos.forEach((tp, info) -> out.put(tp, info.offset()));
            return out;
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    /** Escribe los offsets confirmados del grupo (alterConsumerGroupOffsets). */
    public void alterGroupOffsets(String groupId, Map<TopicPartition, Long> targetOffsets) {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = new java.util.HashMap<>();
            targetOffsets.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
            AlterConsumerGroupOffsetsResult result = admin.alterConsumerGroupOffsets(groupId, offsets);
            result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    /** Cantidad de particiones de cada tópico pedido que existe. */
    public Map<String, Integer> topicPartitionCounts(Collection<String> topicNames) {
        if (topicNames.isEmpty()) {
            return Map.of();
        }
        try {
            DescribeTopicsResult result = admin.describeTopics(topicNames);
            Map<String, TopicDescription> described =
                    result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Map<String, Integer> out = new java.util.HashMap<>();
            described.forEach((name, d) -> out.put(name, d.partitions().size()));
            return out;
        } catch (Exception e) {
            throw unavailable(e);
        }
    }

    private KafkaUnavailableException unavailable(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new KafkaUnavailableException("Kafka no esta disponible", e);
    }

    private NewTopic toNewTopic(String name, TopicSpec spec) {
        return new NewTopic(name, spec.getPartitions(), spec.getReplicationFactor().shortValue())
                .configs(spec.toConfigs());
    }

    private void warnOnConfigDifferences(String name, TopicSpec spec) {
        Map<String, String> expected = spec.toConfigs();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        try {
            DescribeConfigsResult result = admin.describeConfigs(List.of(resource), new DescribeConfigsOptions());
            Config config = result.all().get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get(resource);
            for (Map.Entry<String, String> wanted : expected.entrySet()) {
                ConfigEntry actual = config == null ? null : config.get(wanted.getKey());
                String actualValue = actual == null ? null : String.valueOf(actual.value());
                if (!wanted.getValue().equals(actualValue)) {
                    log.warn(
                            "Topico {}: config {} esperado {} pero es {}. No se modifica el topico del sistema.",
                            name, wanted.getKey(), wanted.getValue(), actualValue);
                }
            }
        } catch (Exception e) {
            log.warn("No se pudieron leer las configs del topico {}: {}", name, e.getMessage());
        }
    }
}
