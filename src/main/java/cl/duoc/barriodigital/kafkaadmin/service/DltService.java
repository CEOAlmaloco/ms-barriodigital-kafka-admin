package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaRecordClient.ConsumedRecord;
import cl.duoc.barriodigital.kafkaadmin.web.DltMessageView;
import cl.duoc.barriodigital.kafkaadmin.web.DltMessagesView;
import cl.duoc.barriodigital.kafkaadmin.web.DltPartitionView;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayRequest;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayRequest.OffsetRef;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayResponse;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayResultView;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * Listado y replay de las DLT del sistema (EP3-21 / EP3-03 sección 7).
 * No usa {@code AdminClient} ni Kafka clients directamente: pasa por
 * {@link KafkaAdminService} y {@link KafkaRecordClient}.
 */
@Service
public class DltService {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REPLAY_BUDGET = Duration.ofSeconds(23);
    private static final Duration SEND_RESERVE = Duration.ofSeconds(15);
    private static final Duration SEARCH_TIMEOUT = Duration.ofSeconds(5);
    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 100;
    private static final int MAX_REPLAY = 100;

    private static final String HDR_EXCEPTION = "kafka_dlt-exception-fqcn";
    private static final String HDR_CAUSE = "kafka_dlt-exception-cause-fqcn";
    private static final String HDR_MESSAGE = "kafka_dlt-exception-message";
    private static final String HDR_ORIGINAL_TOPIC = "kafka_dlt-original-topic";
    private static final String HDR_ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    private static final String HDR_ORIGINAL_OFFSET = "kafka_dlt-original-offset";
    private static final String HDR_ORIGINAL_TIMESTAMP = "kafka_dlt-original-timestamp";
    private static final String HDR_CONSUMER_GROUP = "kafka_dlt-original-consumer-group";
    private static final String HDR_REPLAYED_FROM = "x-replayed-from-dlt";
    private static final String HDR_REPLAYED_AT = "x-replayed-at";

    private final KafkaAdminService kafkaAdminService;
    private final KafkaRecordClient recordClient;
    private final KafkaTopicsProperties topicsProperties;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public DltService(
            KafkaAdminService kafkaAdminService,
            KafkaRecordClient recordClient,
            KafkaTopicsProperties topicsProperties,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.kafkaAdminService = kafkaAdminService;
        this.recordClient = recordClient;
        this.topicsProperties = topicsProperties;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /** GET /api/kafka/dlt/{name}/messages */
    public DltMessagesView listMessages(String name, Integer partition, Long fromOffset, Integer limit) {
        requireAvailable();
        String dlt = requireSystemDlt(name);
        if (fromOffset != null && partition == null) {
            throw badRequest("fromOffset exige partition");
        }
        int pageSize = limit == null ? DEFAULT_LIMIT : limit;
        if (pageSize < 1 || pageSize > MAX_LIMIT) {
            throw badRequest("limit debe estar entre 1 y 100");
        }

        TopicDescription description = requireTopic(dlt);
        int partitionCount = description.partitions().size();
        List<Integer> partitions = resolvePartitions(partition, partitionCount);

        Map<TopicPartition, Long> earliest = kafkaAdminService.earliestOffsets(topicPartitions(dlt, partitions));
        Map<TopicPartition, Long> latest = kafkaAdminService.latestOffsets(topicPartitions(dlt, partitions));

        Map<Integer, Long> starts = new LinkedHashMap<>();
        Map<Integer, Long> ends = new LinkedHashMap<>();
        for (Integer p : partitions) {
            TopicPartition tp = new TopicPartition(dlt, p);
            long earliestOffset = earliest.getOrDefault(tp, 0L);
            long latestOffset = latest.getOrDefault(tp, 0L);
            long start = fromOffset == null ? earliestOffset : Math.max(fromOffset, earliestOffset);
            starts.put(p, start);
            ends.put(p, latestOffset);
        }

        Map<Integer, Long> readableStarts = new LinkedHashMap<>();
        starts.forEach((p, start) -> {
            if (start < ends.get(p)) {
                readableStarts.put(p, start);
            }
        });

        List<ConsumedRecord> records = readableStarts.isEmpty()
                ? List.of()
                : recordClient.readAssigned(dlt, readableStarts, pageSize, READ_TIMEOUT);

        Map<Integer, List<ConsumedRecord>> byPartition = records.stream()
                .collect(Collectors.groupingBy(ConsumedRecord::partition, LinkedHashMap::new, Collectors.toList()));

        List<DltPartitionView> views = new ArrayList<>();
        for (Integer p : partitions) {
            List<ConsumedRecord> page = byPartition.getOrDefault(p, List.of());
            long start = starts.get(p);
            long end = ends.get(p);
            Long nextOffset;
            if (start >= end) {
                nextOffset = null;
            } else if (page.isEmpty()) {
                nextOffset = start;
            } else if (page.size() < pageSize) {
                nextOffset = null;
            } else {
                long candidate = page.get(page.size() - 1).offset() + 1;
                nextOffset = candidate >= end ? null : candidate;
            }
            views.add(new DltPartitionView(
                    p,
                    nextOffset,
                    page.stream().map(this::toMessageView).toList()
            ));
        }
        return new DltMessagesView(dlt, views);
    }

    /** POST /api/kafka/dlt/{name}/replay */
    public DltReplayResponse replay(String name, DltReplayRequest request) {
        requireAvailable();
        String dlt = requireSystemDlt(name);
        Selection selection = parseSelection(request);
        Instant deadline = clock.instant().plus(REPLAY_BUDGET);

        TopicDescription description = requireTopic(dlt);
        int partitionCount = description.partitions().size();
        List<Integer> allPartitions = new ArrayList<>();
        for (int p = 0; p < partitionCount; p++) {
            allPartitions.add(p);
        }

        List<ReplayItem> items;
        if (selection.byOffsets()) {
            items = resolveByOffsets(dlt, selection.offsets(), allPartitions);
        } else {
            items = resolveByEventIds(dlt, selection.eventIds(), allPartitions, deadline);
        }

        items.sort(Comparator
                .comparing((ReplayItem i) -> i.partition == null ? Integer.MAX_VALUE : i.partition)
                .thenComparing(i -> i.offset == null ? Long.MAX_VALUE : i.offset));

        String targetTopic = items.stream()
                .map(i -> i.originalTopic)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(inferOriginalTopic(dlt));

        boolean stopped = false;
        for (ReplayItem item : items) {
            if (item.status != null) {
                continue;
            }
            if (stopped) {
                item.status = "NO_INTENTADO";
                item.reason = "operacion detenida";
                continue;
            }
            Duration remaining = Duration.between(clock.instant(), deadline);
            if (remaining.compareTo(SEND_RESERVE) < 0) {
                item.status = "NO_INTENTADO";
                item.reason = "tiempo insuficiente";
                stopped = true;
                continue;
            }
            if (item.originalTopic == null || item.originalPartition == null || item.record == null) {
                item.status = "OMITIDO";
                item.reason = "sin destino";
                continue;
            }
            try {
                Headers outbound = replayHeaders(item.record.headers(), dlt, clock.instant());
                recordClient.sendConfirmed(
                        item.originalTopic,
                        item.originalPartition,
                        item.record.key(),
                        item.record.value(),
                        clock.instant(),
                        outbound,
                        SEND_RESERVE
                );
                item.status = "REENVIADO";
                item.reason = null;
                targetTopic = item.originalTopic;
            } catch (TimeoutException e) {
                item.status = "FALLIDO";
                item.reason = "sin confirmacion";
                stopped = true;
            } catch (Exception e) {
                item.status = "FALLIDO";
                item.reason = e.getMessage() == null ? "error de envio" : truncate(e.getMessage(), 200);
                stopped = true;
            }
        }

        List<DltReplayResultView> results = items.stream()
                .map(i -> new DltReplayResultView(i.partition, i.offset, i.eventId, i.status, i.reason))
                .toList();
        int resent = (int) results.stream().filter(r -> "REENVIADO".equals(r.status())).count();
        int skipped = (int) results.stream().filter(r -> "OMITIDO".equals(r.status())).count();
        int failed = (int) results.stream().filter(r -> "FALLIDO".equals(r.status())).count();
        int notAttempted = (int) results.stream().filter(r -> "NO_INTENTADO".equals(r.status())).count();
        return new DltReplayResponse(dlt, targetTopic, resent, skipped, failed, notAttempted, results);
    }

    private List<ReplayItem> resolveByOffsets(String dlt, List<OffsetRef> offsets, List<Integer> partitions) {
        Map<TopicPartition, Long> earliest = kafkaAdminService.earliestOffsets(topicPartitions(dlt, partitions));
        Map<TopicPartition, Long> latest = kafkaAdminService.latestOffsets(topicPartitions(dlt, partitions));

        Map<Integer, Long> minNeeded = new HashMap<>();
        Map<Integer, Long> maxNeeded = new HashMap<>();
        for (OffsetRef ref : offsets) {
            if (ref.partition() == null || ref.offset() == null) {
                throw badRequest("Cada offset requiere partition y offset");
            }
            if (!partitions.contains(ref.partition())) {
                throw badRequest("La particion no existe");
            }
            minNeeded.merge(ref.partition(), ref.offset(), Math::min);
            maxNeeded.merge(ref.partition(), ref.offset(), Math::max);
        }

        Map<Integer, Long> starts = new LinkedHashMap<>();
        int limitPerPartition = DEFAULT_LIMIT;
        for (Map.Entry<Integer, Long> entry : minNeeded.entrySet()) {
            TopicPartition tp = new TopicPartition(dlt, entry.getKey());
            long earliestOffset = earliest.getOrDefault(tp, 0L);
            long latestOffset = latest.getOrDefault(tp, 0L);
            long start = Math.max(entry.getValue(), earliestOffset);
            if (start < latestOffset) {
                starts.put(entry.getKey(), start);
                long span = maxNeeded.get(entry.getKey()) - start + 1;
                limitPerPartition = Math.max(
                        limitPerPartition,
                        (int) Math.min(MAX_LIMIT, Math.max(span, 1))
                );
            }
        }

        Map<String, ConsumedRecord> found = new HashMap<>();
        if (!starts.isEmpty()) {
            List<ConsumedRecord> records = recordClient.readAssigned(dlt, starts, limitPerPartition, READ_TIMEOUT);
            for (ConsumedRecord record : records) {
                found.put(record.partition() + ":" + record.offset(), record);
            }
        }

        // Si el mismo eventId aparece en varios offsets pedidos, solo la ocurrencia más reciente se reenvía.
        Map<String, OffsetRef> newestByEventId = new LinkedHashMap<>();
        Map<String, ConsumedRecord> recordByKey = new HashMap<>();
        for (OffsetRef ref : offsets) {
            ConsumedRecord record = found.get(ref.partition() + ":" + ref.offset());
            if (record == null) {
                continue;
            }
            String eventId = readEventId(record.value());
            if (eventId == null) {
                continue;
            }
            OffsetRef previous = newestByEventId.get(eventId);
            if (previous == null || isNewer(record, found.get(previous.partition() + ":" + previous.offset()))) {
                newestByEventId.put(eventId, ref);
                recordByKey.put(eventId, record);
            }
        }

        List<ReplayItem> items = new ArrayList<>();
        for (OffsetRef ref : offsets) {
            ReplayItem item = new ReplayItem();
            item.partition = ref.partition();
            item.offset = ref.offset();
            ConsumedRecord record = found.get(ref.partition() + ":" + ref.offset());
            if (record == null) {
                item.status = "OMITIDO";
                item.reason = "offset inexistente";
                items.add(item);
                continue;
            }
            item.record = record;
            item.eventId = readEventId(record.value());
            item.originalTopic = header(record.headers(), HDR_ORIGINAL_TOPIC);
            item.originalPartition = parseInt(header(record.headers(), HDR_ORIGINAL_PARTITION));
            if (item.eventId != null) {
                OffsetRef keeper = newestByEventId.get(item.eventId);
                if (keeper != null
                        && !(Objects.equals(keeper.partition(), ref.partition())
                                && Objects.equals(keeper.offset(), ref.offset()))) {
                    item.status = "OMITIDO";
                    item.reason = "eventId duplicado";
                }
            }
            items.add(item);
        }
        return items;
    }

    private List<ReplayItem> resolveByEventIds(
            String dlt,
            List<String> eventIds,
            List<Integer> partitions,
            Instant deadline
    ) {
        Instant searchDeadline = clock.instant().plus(SEARCH_TIMEOUT);
        if (searchDeadline.isAfter(deadline)) {
            searchDeadline = deadline;
        }
        Duration searchBudget = Duration.between(clock.instant(), searchDeadline);
        if (searchBudget.isNegative() || searchBudget.isZero()) {
            return eventIds.stream().map(id -> {
                ReplayItem item = new ReplayItem();
                item.eventId = id;
                item.status = "NO_INTENTADO";
                item.reason = "busqueda agotada";
                return item;
            }).toList();
        }

        Map<TopicPartition, Long> earliest = kafkaAdminService.earliestOffsets(topicPartitions(dlt, partitions));
        Map<TopicPartition, Long> latest = kafkaAdminService.latestOffsets(topicPartitions(dlt, partitions));
        Map<Integer, Long> starts = new LinkedHashMap<>();
        long totalToScan = 0;
        for (Integer p : partitions) {
            TopicPartition tp = new TopicPartition(dlt, p);
            long start = earliest.getOrDefault(tp, 0L);
            long end = latest.getOrDefault(tp, 0L);
            if (start < end) {
                starts.put(p, start);
                totalToScan += (end - start);
            }
        }

        Set<String> wanted = eventIds.stream().filter(Objects::nonNull).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
        Map<String, ConsumedRecord> newest = new LinkedHashMap<>();
        boolean searchExhausted = false;
        if (!starts.isEmpty() && !wanted.isEmpty()) {
            // El tope real de la búsqueda es el tiempo (5 s), no el limit de paginación del listado.
            int limit = (int) Math.min(Math.max(totalToScan, 1), 50_000);
            List<ConsumedRecord> records = recordClient.readAssigned(dlt, starts, limit, searchBudget);
            long scanned = records.size();
            for (ConsumedRecord record : records) {
                String eventId = readEventId(record.value());
                if (eventId == null || !wanted.contains(eventId)) {
                    continue;
                }
                ConsumedRecord previous = newest.get(eventId);
                if (previous == null || isNewer(record, previous)) {
                    newest.put(eventId, record);
                }
            }
            boolean incompleteScan = scanned < totalToScan;
            boolean timedOut = !Duration.between(clock.instant(), searchDeadline).isPositive();
            searchExhausted = incompleteScan && (timedOut || !wanted.stream().allMatch(newest::containsKey));
            if (wanted.stream().allMatch(newest::containsKey)) {
                searchExhausted = false;
            }
        }

        List<ReplayItem> items = new ArrayList<>();
        for (String eventId : eventIds) {
            ReplayItem item = new ReplayItem();
            item.eventId = eventId;
            ConsumedRecord record = newest.get(eventId);
            if (record == null) {
                item.status = searchExhausted ? "NO_INTENTADO" : "OMITIDO";
                item.reason = searchExhausted ? "busqueda agotada" : "eventId no encontrado";
                items.add(item);
                continue;
            }
            item.record = record;
            item.partition = record.partition();
            item.offset = record.offset();
            item.originalTopic = header(record.headers(), HDR_ORIGINAL_TOPIC);
            item.originalPartition = parseInt(header(record.headers(), HDR_ORIGINAL_PARTITION));
            items.add(item);
        }
        return items;
    }

    private Selection parseSelection(DltReplayRequest request) {
        if (request == null) {
            throw badRequest("Indica offsets o eventIds, no ambos");
        }
        boolean hasOffsets = request.offsets() != null && !request.offsets().isEmpty();
        boolean hasEventIds = request.eventIds() != null && !request.eventIds().isEmpty();
        if (hasOffsets == hasEventIds) {
            throw badRequest("Indica offsets o eventIds, no ambos");
        }
        if (hasOffsets) {
            if (request.offsets().size() > MAX_REPLAY) {
                throw badRequest("El replay admite hasta 100 mensajes");
            }
            return Selection.offsets(request.offsets());
        }
        if (request.eventIds().size() > MAX_REPLAY) {
            throw badRequest("El replay admite hasta 100 mensajes");
        }
        return Selection.eventIds(request.eventIds());
    }

    private DltMessageView toMessageView(ConsumedRecord record) {
        Headers headers = record.headers();
        String value = record.value();
        return new DltMessageView(
                record.partition(),
                record.offset(),
                record.key(),
                record.timestamp(),
                readEventId(value),
                readEventType(value),
                value,
                header(headers, HDR_ORIGINAL_TOPIC),
                parseInt(header(headers, HDR_ORIGINAL_PARTITION)),
                parseLong(header(headers, HDR_ORIGINAL_OFFSET)),
                parseInstant(header(headers, HDR_ORIGINAL_TIMESTAMP)),
                header(headers, HDR_CONSUMER_GROUP),
                header(headers, HDR_EXCEPTION),
                header(headers, HDR_CAUSE),
                header(headers, HDR_MESSAGE)
        );
    }

    private Headers replayHeaders(Headers source, String dlt, Instant at) {
        RecordHeaders out = new RecordHeaders();
        if (source != null) {
            for (Header header : source) {
                if (header.key() != null && header.key().startsWith("kafka_dlt-")) {
                    continue;
                }
                out.add(header);
            }
        }
        out.add(HDR_REPLAYED_FROM, dlt.getBytes(StandardCharsets.UTF_8));
        out.add(HDR_REPLAYED_AT, at.toString().getBytes(StandardCharsets.UTF_8));
        return out;
    }

    private void requireAvailable() {
        if (!kafkaAdminService.isAvailable()) {
            throw new KafkaUnavailableException("Kafka no esta disponible");
        }
    }

    private String requireSystemDlt(String name) {
        if (name == null || name.isBlank() || !systemDltNames().contains(name)) {
            throw badRequest("El nombre no corresponde a una DLT del sistema");
        }
        return name;
    }

    private Set<String> systemDltNames() {
        return topicsProperties.getTopics().keySet().stream()
                .filter(n -> n.endsWith(".DLT"))
                .collect(Collectors.toSet());
    }

    private TopicDescription requireTopic(String name) {
        Map<String, TopicDescription> described = kafkaAdminService.describeTopics(List.of(name));
        TopicDescription description = described.get(name);
        if (description == null) {
            throw badRequest("El nombre no corresponde a una DLT del sistema");
        }
        return description;
    }

    private List<Integer> resolvePartitions(Integer partition, int partitionCount) {
        if (partition == null) {
            List<Integer> all = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                all.add(i);
            }
            return all;
        }
        if (partition < 0 || partition >= partitionCount) {
            throw badRequest("La particion no existe");
        }
        return List.of(partition);
    }

    private static List<TopicPartition> topicPartitions(String topic, List<Integer> partitions) {
        return partitions.stream().map(p -> new TopicPartition(topic, p)).toList();
    }

    private String readEventId(String value) {
        return readJsonField(value, "eventId");
    }

    private String readEventType(String value) {
        return readJsonField(value, "eventType");
    }

    private String readJsonField(String value, String field) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(value);
            JsonNode child = node.get(field);
            return child == null || child.isNull() ? null : child.asText();
        } catch (Exception e) {
            return null;
        }
    }

    private static String header(Headers headers, String name) {
        if (headers == null) {
            return null;
        }
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer parseInt(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLong(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Long.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Instant parseInstant(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(raw.trim()));
        } catch (NumberFormatException ignored) {
            // Spring Kafka a veces serializa el instante como texto ISO.
        }
        try {
            return Instant.parse(raw.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isNewer(ConsumedRecord candidate, ConsumedRecord previous) {
        if (previous == null) {
            return true;
        }
        int byTime = candidate.timestamp().compareTo(previous.timestamp());
        if (byTime != 0) {
            return byTime > 0;
        }
        int byPartition = Integer.compare(candidate.partition(), previous.partition());
        if (byPartition != 0) {
            return byPartition > 0;
        }
        return candidate.offset() > previous.offset();
    }

    static String inferOriginalTopic(String dltName) {
        if (dltName == null || !dltName.endsWith(".DLT")) {
            return dltName;
        }
        String without = dltName.substring(0, dltName.length() - 4);
        int lastDot = without.lastIndexOf('.');
        return lastDot < 0 ? without : without.substring(0, lastDot);
    }

    private static String truncate(String value, int max) {
        return value.length() <= max ? value : value.substring(0, max);
    }

    private static ResponseStatusException badRequest(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }

    private record Selection(List<OffsetRef> offsets, List<String> eventIds) {
        static Selection offsets(List<OffsetRef> offsets) {
            return new Selection(offsets, null);
        }

        static Selection eventIds(List<String> eventIds) {
            return new Selection(null, eventIds);
        }

        boolean byOffsets() {
            return offsets != null;
        }
    }

    private static final class ReplayItem {
        Integer partition;
        Long offset;
        String eventId;
        String originalTopic;
        Integer originalPartition;
        ConsumedRecord record;
        String status;
        String reason;
    }
}
