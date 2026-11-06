package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.web.CreateTopicRequest;
import cl.duoc.barriodigital.kafkaadmin.web.TopicDetailView;
import cl.duoc.barriodigital.kafkaadmin.web.TopicDetailView.ConfigView;
import cl.duoc.barriodigital.kafkaadmin.web.TopicDetailView.PartitionView;
import cl.duoc.barriodigital.kafkaadmin.web.TopicSummaryView;
import cl.duoc.barriodigital.kafkaadmin.web.UpdatePartitionsRequest;
import cl.duoc.barriodigital.kafkaadmin.web.UpdateTopicConfigsRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Tópicos del Admin: consulta, alta, cambio de configuraciones y particiones,
 * y eliminación (EP3-03, 2 a 5). No usa {@code AdminClient} directamente:
 * todo pasa por {@link KafkaAdminService}.
 */
@Service
public class TopicService {

    private static final Pattern NAME_PATTERN = Pattern.compile("^[A-Za-z0-9._-]{1,249}$");
    private static final int MAX_PARTITIONS = 12;
    private static final int MAX_REPLICATION_FACTOR = 3;

    /** Configuraciones que la API permite ver/editar (EP3-03, 3.3). */
    private static final Set<String> EDITABLE_CONFIGS = Set.of(
            "retention.ms", "cleanup.policy", "min.insync.replicas",
            "min.compaction.lag.ms", "segment.ms");

    private static final Set<String> CLEANUP_POLICIES = Set.of("delete", "compact", "compact,delete");

    private final KafkaAdminService kafkaAdminService;
    private final KafkaTopicsProperties topicsProperties;

    public TopicService(KafkaAdminService kafkaAdminService, KafkaTopicsProperties topicsProperties) {
        this.kafkaAdminService = kafkaAdminService;
        this.topicsProperties = topicsProperties;
    }

    /** GET /api/kafka/topics — lista con bandera system, sin tópicos internos. */
    public List<TopicSummaryView> listTopics() {
        Map<String, TopicDescription> descriptions = kafkaAdminService.describeTopics(kafkaAdminService.listTopicNames());
        List<TopicSummaryView> out = new ArrayList<>();
        descriptions.forEach((name, d) -> out.add(new TopicSummaryView(
                name, d.partitions().size(), replicationFactor(d), isSystem(name))));
        out.sort((a, b) -> a.name().compareTo(b.name()));
        return out;
    }

    /** GET /api/kafka/topics/{name} — particiones y configuraciones del contrato. */
    public TopicDetailView describeTopic(String name) {
        validateName(name);
        TopicDescription description = requireTopic(name);
        return buildDetail(name, description);
    }

    /** POST /api/kafka/topics — alta con las validaciones del contrato. */
    public TopicDetailView createTopic(CreateTopicRequest request) {
        if (request == null) {
            throw badRequest("El cuerpo es obligatorio");
        }
        String name = request.name() == null ? "" : request.name().trim();
        validateName(name);
        if (request.partitions() == null || request.partitions() < 1 || request.partitions() > MAX_PARTITIONS) {
            throw badRequest("partitions debe ser un entero de 1 a 12");
        }
        if (request.replicationFactor() == null
                || request.replicationFactor() < 1 || request.replicationFactor() > MAX_REPLICATION_FACTOR) {
            throw badRequest("replicationFactor debe ser un entero de 1 a 3");
        }

        Set<String> existing = kafkaAdminService.listTopicNames();
        if (existing.contains(name)) {
            throw conflict("El topico ya existe");
        }
        if (name.endsWith(".DLT")) {
            throw conflict("El nombre esta reservado para el sistema");
        }
        String normalized = name.replace('.', '_');
        boolean collision = existing.stream()
                .anyMatch(other -> other.replace('.', '_').equals(normalized));
        if (collision) {
            throw conflict("El nombre no esta disponible");
        }
        if (request.replicationFactor() > kafkaAdminService.brokerCount()) {
            throw badRequest("replicationFactor no puede superar el numero de brokers del cluster");
        }

        Map<String, String> configs = validateConfigsForCreate(request.configs(), request.replicationFactor());
        try {
            kafkaAdminService.createTopic(name, request.partitions(), request.replicationFactor(), configs);
        } catch (TopicExistsException | InvalidTopicException e) {
            throw conflict("El nombre no esta disponible");
        }
        return describeTopic(name);
    }

    /** PATCH /api/kafka/topics/{name}/configs — cambio parcial con SET. */
    public TopicDetailView updateConfigs(String name, UpdateTopicConfigsRequest request) {
        validateName(name);
        TopicDescription description = requireTopic(name);
        if (request == null || request.configs() == null) {
            throw badRequest("configs es obligatorio");
        }
        boolean system = isSystem(name);
        int replicationFactor = replicationFactor(description);
        Map<String, String> validated = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : request.configs().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                throw badRequest("El valor de " + key + " no puede ser null");
            }
            if (!EDITABLE_CONFIGS.contains(key)) {
                throw badRequest("La configuracion " + key + " no es editable");
            }
            if (system && !"retention.ms".equals(key)) {
                throw conflict("La configuracion no es editable en un topico del sistema");
            }
            validateConfigValue(key, value, system, name, replicationFactor);
            validated.put(key, value);
        }
        kafkaAdminService.updateTopicConfigs(name, validated);
        return describeTopic(name);
    }

    /** PATCH /api/kafka/topics/{name}/partitions — solo aumenta, nunca en el sistema. */
    public TopicDetailView updatePartitions(String name, UpdatePartitionsRequest request) {
        validateName(name);
        TopicDescription description = requireTopic(name);
        if (isSystem(name)) {
            throw conflict("El topico es del sistema y no admite mas particiones");
        }
        int current = description.partitions().size();
        if (request == null || request.partitions() == null
                || request.partitions() <= current || request.partitions() > MAX_PARTITIONS) {
            throw badRequest("partitions debe ser mayor que " + current + " y no superar 12");
        }
        kafkaAdminService.increaseTopicPartitions(name, request.partitions());
        return describeTopic(name);
    }

    /** DELETE /api/kafka/topics/{name} — 204, con las restricciones del contrato. */
    public void deleteTopic(String name) {
        validateName(name);
        requireTopic(name);
        if (isSystem(name)) {
            throw conflict("El topico es del sistema y no se puede eliminar");
        }
        if (kafkaAdminService.topicHasActiveConsumers(name)) {
            throw conflict("El topico tiene consumidores activos");
        }
        kafkaAdminService.deleteTopic(name);
    }

    private TopicDetailView buildDetail(String name, TopicDescription description) {
        boolean system = isSystem(name);
        List<PartitionView> partitions = new ArrayList<>();
        for (var p : description.partitions()) {
            partitions.add(new PartitionView(p.partition(),
                    p.leader() == null ? null : p.leader().id(),
                    ids(p.replicas()), ids(p.isr())));
        }
        List<ConfigView> configs = new ArrayList<>();
        for (KafkaAdminService.TopicConfigEntry entry : kafkaAdminService.topicConfigs(name)) {
            if (!includeConfig(entry)) {
                continue;
            }
            configs.add(new ConfigView(entry.name(), entry.value(), entry.source(), isEditable(entry.name(), system)));
        }
        return new TopicDetailView(name, system, partitions, configs);
    }

    /** Lista de la sección 3.3, más las que no están en su valor por defecto. */
    private boolean includeConfig(KafkaAdminService.TopicConfigEntry entry) {
        return EDITABLE_CONFIGS.contains(entry.name())
                || "message.timestamp.type".equals(entry.name())
                || !"DEFAULT".equals(entry.source());
    }

    private boolean isEditable(String config, boolean system) {
        if (!EDITABLE_CONFIGS.contains(config)) {
            return false;
        }
        return !system || "retention.ms".equals(config);
    }

    private TopicDescription requireTopic(String name) {
        if (!kafkaAdminService.listTopicNames().contains(name)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El topico no existe");
        }
        Map<String, TopicDescription> described = kafkaAdminService.describeTopics(Set.of(name));
        TopicDescription description = described.get(name);
        if (description == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El topico no existe");
        }
        return description;
    }

    private Map<String, String> validateConfigsForCreate(Map<String, String> configs, int replicationFactor) {
        Map<String, String> out = new LinkedHashMap<>();
        if (configs == null) {
            return out;
        }
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                throw badRequest("El valor de " + key + " no puede ser null");
            }
            if (!EDITABLE_CONFIGS.contains(key)) {
                throw badRequest("La configuracion " + key + " no es editable");
            }
            validateConfigValue(key, value, false, null, replicationFactor);
            out.put(key, value);
        }
        return out;
    }

    /** Rangos de la sección 3.3: Admin completo, sistema solo retention.ms acotado. */
    private void validateConfigValue(String key, String value, boolean system, String topic, int replicationFactor) {
        switch (key) {
            case "retention.ms" -> {
                long[] range = retentionRange(system, topic);
                long parsed = parseLong(key, value);
                if (parsed < range[0] || parsed > range[1]) {
                    throw badRequest("retention.ms fuera de rango");
                }
            }
            case "cleanup.policy" -> {
                if (!CLEANUP_POLICIES.contains(value.trim())) {
                    throw badRequest("cleanup.policy debe ser delete, compact o compact,delete");
                }
            }
            case "min.insync.replicas" -> {
                long parsed = parseLong(key, value);
                long max = Math.max(1, Math.min(2, replicationFactor - 1));
                if (parsed < 1 || parsed > max) {
                    throw badRequest("min.insync.replicas fuera de rango");
                }
            }
            case "min.compaction.lag.ms" -> {
                long parsed = parseLong(key, value);
                if (parsed < 0 || parsed > 604800000L) {
                    throw badRequest("min.compaction.lag.ms fuera de rango");
                }
            }
            case "segment.ms" -> {
                long parsed = parseLong(key, value);
                if (parsed < 3600000L || parsed > 604800000L) {
                    throw badRequest("segment.ms fuera de rango");
                }
            }
            default -> throw badRequest("La configuracion " + key + " no es editable");
        }
    }

    /** Rangos de retention.ms por tipo de tópico (EP3-03, 3.3). */
    private long[] retentionRange(boolean system, String topic) {
        if (!system) {
            return new long[] {3600000L, 2592000000L};
        }
        if (topic != null && topic.endsWith(".DLT")) {
            return new long[] {604800000L, 1209600000L};
        }
        if ("requests.events".equals(topic)) {
            return new long[] {259200000L, 604800000L};
        }
        if ("audit.timeline".equals(topic)) {
            return new long[] {1209600000L, 2592000000L};
        }
        return new long[] {3600000L, 2592000000L};
    }

    private long parseLong(String key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw badRequest(key + " debe ser un entero");
        }
    }

    private void validateName(String name) {
        if (name == null || !NAME_PATTERN.matcher(name).matches()
                || name.startsWith("__") || name.equals(".") || name.equals("..")) {
            throw badRequest("Nombre de topico invalido");
        }
    }

    private boolean isSystem(String name) {
        return topicsProperties.getTopics().containsKey(name);
    }

    private int replicationFactor(TopicDescription description) {
        return description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
    }

    private List<Integer> ids(List<Node> nodes) {
        List<Integer> out = new ArrayList<>();
        for (Node node : nodes) {
            out.add(node.id());
        }
        return out;
    }

    private ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }

    private ResponseStatusException conflict(String message) {
        return new ResponseStatusException(HttpStatus.CONFLICT, message);
    }
}
