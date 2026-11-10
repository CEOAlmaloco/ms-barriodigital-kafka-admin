package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaGroupsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaGroupsProperties.GroupSpec;
import cl.duoc.barriodigital.kafkaadmin.messaging.AuditEventFactory;
import cl.duoc.barriodigital.kafkaadmin.messaging.AuditPublisher;
import cl.duoc.barriodigital.kafkaadmin.web.GroupDetailView;
import cl.duoc.barriodigital.kafkaadmin.web.GroupDetailView.GroupTopicView;
import cl.duoc.barriodigital.kafkaadmin.web.GroupDetailView.PartitionLagView;
import cl.duoc.barriodigital.kafkaadmin.web.GroupSummaryView;
import cl.duoc.barriodigital.kafkaadmin.web.ResetOffsetsRequest;
import cl.duoc.barriodigital.kafkaadmin.web.ResetOffsetsResponse;
import cl.duoc.barriodigital.kafkaadmin.web.ResetOffsetsResponse.PartitionResetView;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Consumer groups: listado, detalle con lag y reset de offsets (EP3-03, 6).
 * No usa {@code AdminClient} directamente: todo pasa por {@link KafkaAdminService}.
 */
@Service
public class ConsumerGroupService {

    private static final String STATE_DEAD = "DEAD";

    private final KafkaAdminService kafkaAdminService;
    private final KafkaGroupsProperties groupsProperties;
    private final AuditEventFactory auditEvents;
    private final AuditPublisher auditPublisher;

    public ConsumerGroupService(
            KafkaAdminService kafkaAdminService,
            KafkaGroupsProperties groupsProperties,
            AuditEventFactory auditEvents,
            AuditPublisher auditPublisher
    ) {
        this.kafkaAdminService = kafkaAdminService;
        this.groupsProperties = groupsProperties;
        this.auditEvents = auditEvents;
        this.auditPublisher = auditPublisher;
    }

    /** Listado de grupos: los del broker más los del sistema siempre presentes. */
    public List<GroupSummaryView> listGroups() {
        Set<String> brokerIds = kafkaAdminService.listGroupIds();
        Map<String, KafkaAdminService.GroupDescription> described = kafkaAdminService.describeGroups(brokerIds);

        List<GroupSummaryView> out = new ArrayList<>();
        for (String id : groupsProperties.getGroups().keySet()) {
            if (brokerIds.contains(id)) {
                KafkaAdminService.GroupDescription d = described.get(id);
                out.add(new GroupSummaryView(id, d == null ? null : d.state(), d == null ? 0 : d.members(), true));
            } else {
                out.add(new GroupSummaryView(id, STATE_DEAD, 0, true));
            }
        }
        brokerIds.stream()
                .filter(id -> !groupsProperties.getGroups().containsKey(id))
                .sorted()
                .forEach(id -> {
                    KafkaAdminService.GroupDescription d = described.get(id);
                    out.add(new GroupSummaryView(id, d == null ? null : d.state(), d == null ? 0 : d.members(), false));
                });
        return out;
    }

    /** Detalle de un grupo con lag por tópico y partición. */
    public GroupDetailView describeGroup(String groupId) {
        GroupSpec spec = groupsProperties.getGroups().get(groupId);
        boolean system = spec != null;
        if (!system && !kafkaAdminService.listGroupIds().contains(groupId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El grupo no existe");
        }

        KafkaAdminService.GroupDescription d = kafkaAdminService.describeGroups(Set.of(groupId)).get(groupId);
        String state = d == null || d.state() == null ? (system ? STATE_DEAD : null) : d.state();
        int members = d == null ? 0 : d.members();

        Map<TopicPartition, Long> committed = kafkaAdminService.committedOffsets(groupId);
        Set<String> topicNames = new java.util.LinkedHashSet<>();
        if (system) {
            topicNames.addAll(spec.getTopics());
        } else {
            committed.keySet().stream().map(TopicPartition::topic).forEach(topicNames::add);
        }

        Set<String> existingTopics = existingTopics(topicNames);
        Map<String, Integer> partitionCounts = kafkaAdminService.topicPartitionCounts(existingTopics);

        List<GroupTopicView> topics = new ArrayList<>();
        for (String topic : topicNames) {
            Integer count = partitionCounts.get(topic);
            if (count == null) {
                continue;
            }
            topics.add(buildTopicLag(groupId, topic, count, committed, system));
        }
        return new GroupDetailView(groupId, state, members, system, topics);
    }

    /**
     * Reset de offsets de un grupo para un tópico completo (todas las
     * particiones). El dry run aplica las mismas validaciones que la real.
     */
    public ResetOffsetsResponse resetOffsets(String groupId, ResetOffsetsRequest request) {
        String topic = validateRequest(request);

        GroupSpec spec = groupsProperties.getGroups().get(groupId);
        boolean system = spec != null;
        if (!system && !kafkaAdminService.listGroupIds().contains(groupId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El grupo no existe");
        }
        if (!existingTopics(Set.of(topic)).contains(topic)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El topico no existe");
        }
        KafkaAdminService.GroupDescription d = kafkaAdminService.describeGroups(Set.of(groupId)).get(groupId);
        if (d != null && d.members() > 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "El grupo tiene miembros activos");
        }
        if (system && !spec.getTopics().contains(topic)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "El grupo no consume ese topico");
        }

        Map<String, Integer> counts = kafkaAdminService.topicPartitionCounts(Set.of(topic));
        int partitions = counts.getOrDefault(topic, 0);
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            tps.add(new TopicPartition(topic, i));
        }

        Map<TopicPartition, Long> committed = kafkaAdminService.committedOffsets(groupId);
        Map<TopicPartition, Long> committedInTopic = new HashMap<>();
        committed.forEach((tp, offset) -> {
            if (tp.topic().equals(topic)) {
                committedInTopic.put(tp, offset);
            }
        });
        if (!system && committedInTopic.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "El grupo no tiene offsets en ese topico");
        }

        Strategy strategy = Strategy.valueOf(request.strategy().trim().toUpperCase());
        Map<TopicPartition, Long> ends = kafkaAdminService.latestOffsets(tps);
        Map<TopicPartition, Long> targets = computeTargets(strategy, tps, ends, request.timestamp());

        if (system) {
            checkNoForwardMove(strategy, tps, committedInTopic, targets);
        }

        List<PartitionResetView> rows = new ArrayList<>();
        Map<TopicPartition, Long> toWrite = new HashMap<>();
        for (TopicPartition tp : tps) {
            long end = ends.getOrDefault(tp, 0L);
            long target = targets.getOrDefault(tp, end);
            Long current = committedInTopic.get(tp);
            rows.add(new PartitionResetView(tp.partition(), current, target, Math.max(0, end - target)));
            toWrite.put(tp, target);
        }

        if (!request.dryRun()) {
            kafkaAdminService.alterGroupOffsets(groupId, toWrite);
            Map<String, Object> before = new LinkedHashMap<>();
            Map<String, Object> after = new LinkedHashMap<>();
            List<Map<String, Object>> beforePartitions = new ArrayList<>();
            List<Map<String, Object>> afterPartitions = new ArrayList<>();
            for (PartitionResetView row : rows) {
                Map<String, Object> beforeRow = new LinkedHashMap<>();
                beforeRow.put("partition", row.partition());
                beforeRow.put("offset", row.currentOffset());
                beforePartitions.add(beforeRow);
                Map<String, Object> afterRow = new LinkedHashMap<>();
                afterRow.put("partition", row.partition());
                afterRow.put("offset", row.targetOffset());
                afterPartitions.add(afterRow);
            }
            before.put("partitions", beforePartitions);
            after.put("topic", topic);
            after.put("strategy", strategy.name());
            after.put("partitions", afterPartitions);
            auditPublisher.publish(auditEvents.create("consumer-group", groupId, "offsets-reset", before, after));
        }
        return new ResetOffsetsResponse(groupId, topic, strategy.name(), request.dryRun(), rows);
    }

    private String validateRequest(ResetOffsetsRequest request) {
        if (request == null || request.topic() == null || request.topic().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "topic es obligatorio");
        }
        if (request.dryRun() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "dryRun es obligatorio");
        }
        if (request.strategy() == null
                || !List.of("EARLIEST", "LATEST", "TIMESTAMP").contains(request.strategy().trim().toUpperCase())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "La estrategia debe ser EARLIEST, LATEST o TIMESTAMP");
        }
        if ("TIMESTAMP".equals(request.strategy().trim().toUpperCase())) {
            if (request.timestamp() == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "timestamp es obligatorio con TIMESTAMP");
            }
            if (request.timestamp().isAfter(Instant.now())) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "timestamp no puede ser futuro");
            }
        }
        return request.topic().trim();
    }

    private enum Strategy {
        EARLIEST, LATEST, TIMESTAMP
    }

    private Map<TopicPartition, Long> computeTargets(Strategy strategy, List<TopicPartition> tps,
            Map<TopicPartition, Long> ends, Instant timestamp) {
        return switch (strategy) {
            case EARLIEST -> kafkaAdminService.earliestOffsets(tps);
            case LATEST -> ends;
            case TIMESTAMP -> {
                Map<TopicPartition, Long> at = kafkaAdminService.offsetsAtTimestamp(tps, timestamp);
                Map<TopicPartition, Long> out = new HashMap<>();
                for (TopicPartition tp : tps) {
                    long candidate = at.getOrDefault(tp, -1L);
                    long end = ends.getOrDefault(tp, 0L);
                    // Sin mensajes desde la fecha queda en el offset final; nunca supera el final.
                    out.put(tp, candidate < 0 ? end : Math.min(candidate, end));
                }
                yield out;
            }
        };
    }

    /**
     * En los grupos del sistema está prohibido mover offsets hacia adelante.
     * LATEST siempre adelanta; TIMESTAMP adelanta si supera el confirmado, y una
     * partición sin confirmado no admite TIMESTAMP.
     */
    private void checkNoForwardMove(Strategy strategy, List<TopicPartition> tps,
            Map<TopicPartition, Long> committed, Map<TopicPartition, Long> targets) {
        List<Integer> offenders = new ArrayList<>();
        for (TopicPartition tp : tps) {
            Long current = committed.get(tp);
            long target = targets.getOrDefault(tp, 0L);
            if (strategy == Strategy.LATEST) {
                offenders.add(tp.partition());
            } else if (strategy == Strategy.TIMESTAMP && (current == null || target > current)) {
                offenders.add(tp.partition());
            }
        }
        if (!offenders.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "El reset adelantaria offsets en las particiones " + offenders.stream().map(String::valueOf).collect(Collectors.joining(", ")));
        }
    }

    private GroupTopicView buildTopicLag(String groupId, String topic, int partitionCount,
            Map<TopicPartition, Long> committed, boolean system) {
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            tps.add(new TopicPartition(topic, i));
        }
        Map<TopicPartition, Long> ends = kafkaAdminService.latestOffsets(tps);
        Map<TopicPartition, Long> earliest = new HashMap<>();
        if (system) {
            earliest = kafkaAdminService.earliestOffsets(tps);
        }

        List<PartitionLagView> rows = new ArrayList<>();
        for (TopicPartition tp : tps) {
            long end = ends.getOrDefault(tp, 0L);
            Long current = committed.get(tp);
            if (current != null) {
                rows.add(new PartitionLagView(tp.partition(), current, end, Math.max(0, end - current), "COMMITTED"));
            } else if (system) {
                long start = earliest.getOrDefault(tp, 0L);
                rows.add(new PartitionLagView(tp.partition(), null, end, Math.max(0, end - start), "EARLIEST"));
            } else {
                rows.add(new PartitionLagView(tp.partition(), null, end, null, null));
            }
        }
        return new GroupTopicView(topic, rows);
    }

    private Set<String> existingTopics(Collection<String> names) {
        Set<String> all = kafkaAdminService.listTopicNames();
        Set<String> out = new HashSet<>();
        for (String name : names) {
            if (all.contains(name)) {
                out.add(name);
            }
        }
        return out;
    }
}
