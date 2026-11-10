package cl.duoc.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties.TopicSpec;
import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaRecordClient.ConsumedRecord;
import cl.duoc.barriodigital.kafkaadmin.web.DltMessagesView;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayRequest;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayRequest.OffsetRef;
import cl.duoc.barriodigital.kafkaadmin.web.DltReplayResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DltServiceTest {

    private static final String DLT = "requests.events.report.DLT";
    private static final Instant NOW = Instant.parse("2026-11-12T12:00:00Z");

    @Mock
    private KafkaAdminService kafkaAdminService;

    @Mock
    private KafkaRecordClient recordClient;

    private DltService service;

    @BeforeEach
    void setUp() {
        KafkaTopicsProperties topics = new KafkaTopicsProperties();
        Map<String, TopicSpec> map = new java.util.LinkedHashMap<>();
        map.put("requests.events", new TopicSpec());
        map.put(DLT, new TopicSpec());
        map.put("requests.events.audit.DLT", new TopicSpec());
        map.put("audit.timeline.audit.DLT", new TopicSpec());
        topics.setTopics(map);
        service = new DltService(
                kafkaAdminService,
                recordClient,
                topics,
                new ObjectMapper(),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
        when(kafkaAdminService.isAvailable()).thenReturn(true);
        when(kafkaAdminService.describeTopics(any())).thenReturn(Map.of(DLT, topic(DLT, 3)));
    }

    @Test
    void listadoRechazaNombreQueNoEsDltDelSistema() {
        assertStatus(() -> service.listMessages("requests.events", null, null, null), HttpStatus.BAD_REQUEST);
        assertStatus(() -> service.listMessages("inventada.DLT", null, null, null), HttpStatus.BAD_REQUEST);
    }

    @Test
    void listadoExigePartitionSiHayFromOffset() {
        assertStatus(() -> service.listMessages(DLT, null, 10L, null), HttpStatus.BAD_REQUEST);
    }

    @Test
    void listadoRechazaLimitFueraDeRango() {
        assertStatus(() -> service.listMessages(DLT, null, null, 0), HttpStatus.BAD_REQUEST);
        assertStatus(() -> service.listMessages(DLT, null, null, 101), HttpStatus.BAD_REQUEST);
    }

    @Test
    void listadoAjustaFromOffsetAnteriorALaRetencionYDevuelveMensajes() {
        TopicPartition tp = new TopicPartition(DLT, 1);
        when(kafkaAdminService.earliestOffsets(any())).thenReturn(Map.of(tp, 100L));
        when(kafkaAdminService.latestOffsets(any())).thenReturn(Map.of(tp, 102L));
        when(recordClient.readAssigned(eq(DLT), anyMap(), eq(20), any()))
                .thenReturn(List.of(record(1, 100L, "evt-1", "requests.events", 1)));

        DltMessagesView view = service.listMessages(DLT, 1, 50L, null);

        ArgumentCaptor<Map<Integer, Long>> starts = ArgumentCaptor.forClass(Map.class);
        verify(recordClient).readAssigned(eq(DLT), starts.capture(), eq(20), eq(Duration.ofSeconds(5)));
        assertThat(starts.getValue()).containsEntry(1, 100L);
        assertThat(view.dlt()).isEqualTo(DLT);
        assertThat(view.partitions()).hasSize(1);
        assertThat(view.partitions().get(0).messages()).hasSize(1);
        assertThat(view.partitions().get(0).messages().get(0).eventId()).isEqualTo("evt-1");
        assertThat(view.partitions().get(0).messages().get(0).exceptionClass())
                .isEqualTo("org.springframework.kafka.listener.ListenerExecutionFailedException");
        assertThat(view.partitions().get(0).nextOffset()).isNull();
    }

    @Test
    void listadoSinKafkaResponde503() {
        when(kafkaAdminService.isAvailable()).thenReturn(false);
        assertThatThrownBy(() -> service.listMessages(DLT, null, null, null))
                .isInstanceOf(KafkaUnavailableException.class);
    }

    @Test
    void replayRechazaOffsetsYEventIdsJuntosONinguno() {
        assertStatus(() -> service.replay(DLT, new DltReplayRequest(null, null)), HttpStatus.BAD_REQUEST);
        assertStatus(
                () -> service.replay(DLT, new DltReplayRequest(List.of(new OffsetRef(0, 1L)), List.of("a"))),
                HttpStatus.BAD_REQUEST);
    }

    @Test
    void replayRechazaMasDe100() {
        List<OffsetRef> offsets = IntStream.range(0, 101)
                .mapToObj(i -> new OffsetRef(0, (long) i))
                .toList();
        assertStatus(() -> service.replay(DLT, new DltReplayRequest(offsets, null)), HttpStatus.BAD_REQUEST);
    }

    @Test
    void replayPorOffsetPublicaAlTopicoOriginalYQuitaHeadersDlt() throws Exception {
        TopicPartition tp = new TopicPartition(DLT, 1);
        when(kafkaAdminService.earliestOffsets(any())).thenReturn(Map.of(tp, 10L));
        when(kafkaAdminService.latestOffsets(any())).thenReturn(Map.of(tp, 12L));
        when(recordClient.readAssigned(eq(DLT), anyMap(), anyInt(), any()))
                .thenReturn(List.of(record(1, 10L, "evt-1", "requests.events", 1)));

        DltReplayResponse response = service.replay(
                DLT, new DltReplayRequest(List.of(new OffsetRef(1, 10L)), null));

        assertThat(response.resent()).isEqualTo(1);
        assertThat(response.results().get(0).status()).isEqualTo("REENVIADO");
        assertThat(response.targetTopic()).isEqualTo("requests.events");
        verify(recordClient).sendConfirmed(
                eq("requests.events"),
                eq(1),
                eq("key-1"),
                anyString(),
                eq(NOW),
                any(),
                eq(Duration.ofSeconds(15)));
    }

    @Test
    void replayPorOffsetDuplicadoDeEventIdSoloReenviaElMasReciente() throws Exception {
        TopicPartition tp = new TopicPartition(DLT, 0);
        when(kafkaAdminService.earliestOffsets(any())).thenReturn(Map.of(tp, 1L));
        when(kafkaAdminService.latestOffsets(any())).thenReturn(Map.of(tp, 4L));
        when(recordClient.readAssigned(eq(DLT), anyMap(), anyInt(), any())).thenReturn(List.of(
                record(0, 1L, "evt-1", "requests.events", 0, NOW.minusSeconds(20)),
                record(0, 2L, "evt-1", "requests.events", 0, NOW.minusSeconds(5))));

        DltReplayResponse response = service.replay(
                DLT,
                new DltReplayRequest(List.of(new OffsetRef(0, 1L), new OffsetRef(0, 2L)), null));

        assertThat(response.results()).extracting(r -> r.status())
                .containsExactly("OMITIDO", "REENVIADO");
        verify(recordClient).sendConfirmed(eq("requests.events"), eq(0), any(), any(), any(), any(), any());
    }

    @Test
    void replaySeDetieneAnteFallaYMarcaElRestoNoIntentado() throws Exception {
        TopicPartition tp = new TopicPartition(DLT, 0);
        when(kafkaAdminService.earliestOffsets(any())).thenReturn(Map.of(tp, 1L));
        when(kafkaAdminService.latestOffsets(any())).thenReturn(Map.of(tp, 4L));
        when(recordClient.readAssigned(eq(DLT), anyMap(), anyInt(), any())).thenReturn(List.of(
                record(0, 1L, "evt-1", "requests.events", 0),
                record(0, 2L, "evt-2", "requests.events", 0)));
        doThrow(new TimeoutException("timeout"))
                .when(recordClient)
                .sendConfirmed(anyString(), anyInt(), any(), any(), any(), any(), any());

        DltReplayResponse response = service.replay(
                DLT,
                new DltReplayRequest(List.of(new OffsetRef(0, 1L), new OffsetRef(0, 2L)), null));

        assertThat(response.failed()).isEqualTo(1);
        assertThat(response.notAttempted()).isEqualTo(1);
        assertThat(response.results().get(0).status()).isEqualTo("FALLIDO");
        assertThat(response.results().get(0).reason()).isEqualTo("sin confirmacion");
        assertThat(response.results().get(1).status()).isEqualTo("NO_INTENTADO");
    }

    @Test
    void replayPorEventIdNoEncontradoTrasBusquedaCompletaQuedaOmitido() throws Exception {
        TopicPartition tp0 = new TopicPartition(DLT, 0);
        when(kafkaAdminService.earliestOffsets(any())).thenReturn(Map.of(tp0, 0L));
        when(kafkaAdminService.latestOffsets(any())).thenReturn(Map.of(tp0, 0L));

        DltReplayResponse response = service.replay(DLT, new DltReplayRequest(null, List.of("missing")));

        assertThat(response.skipped()).isEqualTo(1);
        assertThat(response.results().get(0).status()).isEqualTo("OMITIDO");
        verify(recordClient, never()).sendConfirmed(any(), anyInt(), any(), any(), any(), any(), any());
    }

    @Test
    void replaySinBrokersResponde503() {
        when(kafkaAdminService.isAvailable()).thenReturn(false);
        assertThatThrownBy(() -> service.replay(DLT, new DltReplayRequest(List.of(new OffsetRef(0, 1L)), null)))
                .isInstanceOf(KafkaUnavailableException.class);
    }

    private static TopicDescription topic(String name, int partitions) {
        Node node = new Node(1, "localhost", 9092);
        List<TopicPartitionInfo> infos = IntStream.range(0, partitions)
                .mapToObj(p -> new TopicPartitionInfo(p, node, List.of(node), List.of(node)))
                .toList();
        return new TopicDescription(name, false, infos);
    }

    private static ConsumedRecord record(
            int partition, long offset, String eventId, String originalTopic, int originalPartition) {
        return record(partition, offset, eventId, originalTopic, originalPartition, NOW.minusSeconds(60));
    }

    private static ConsumedRecord record(
            int partition,
            long offset,
            String eventId,
            String originalTopic,
            int originalPartition,
            Instant receivedAt
    ) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("kafka_dlt-original-topic", originalTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-original-partition", String.valueOf(originalPartition).getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-original-offset", "99".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-original-timestamp", String.valueOf(receivedAt.minusSeconds(10).toEpochMilli())
                .getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-original-consumer-group", "report-group".getBytes(StandardCharsets.UTF_8));
        headers.add(
                "kafka_dlt-exception-fqcn",
                "org.springframework.kafka.listener.ListenerExecutionFailedException".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-cause-fqcn", "java.sql.SQLRecoverableException".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-message", "Oracle no disponible".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-stacktrace", "STACK".getBytes(StandardCharsets.UTF_8));
        String value = "{\"eventId\":\"" + eventId + "\",\"eventType\":\"request.status-changed\"}";
        return new ConsumedRecord(partition, offset, "key-1", value, receivedAt, headers);
    }

    private static void assertStatus(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, HttpStatus status) {
        assertThatThrownBy(call)
                .isInstanceOf(ResponseStatusException.class)
                .extracting(ex -> ((ResponseStatusException) ex).getStatusCode())
                .isEqualTo(status);
    }
}
