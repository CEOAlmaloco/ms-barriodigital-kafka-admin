package cl.duoc.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaGroupsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaGroupsProperties.GroupSpec;
import cl.duoc.barriodigital.kafkaadmin.web.GroupDetailView;
import cl.duoc.barriodigital.kafkaadmin.web.GroupSummaryView;
import cl.duoc.barriodigital.kafkaadmin.web.ResetOffsetsRequest;
import cl.duoc.barriodigital.kafkaadmin.web.ResetOffsetsResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConsumerGroupServiceTest {

    @Mock
    private KafkaAdminService kafkaAdminService;

    private KafkaGroupsProperties groupsProperties;
    private ConsumerGroupService service;

    @BeforeEach
    void setUp() {
        groupsProperties = new KafkaGroupsProperties();
        Map<String, GroupSpec> groups = new java.util.LinkedHashMap<>();
        groups.put("report-group", groupSpec("requests.events"));
        GroupSpec audit = groupSpec();
        audit.setTopics(List.of("requests.events", "audit.timeline"));
        groups.put("audit-group", audit);
        groupsProperties.setGroups(groups);
        service = new ConsumerGroupService(kafkaAdminService, groupsProperties);
    }

    private GroupSpec groupSpec(String... topics) {
        GroupSpec spec = new GroupSpec();
        spec.setTopics(List.of(topics));
        return spec;
    }

    private void brokerTieneGrupos(Set<String> ids, Map<String, KafkaAdminService.GroupDescription> described) {
        when(kafkaAdminService.listGroupIds()).thenReturn(ids);
        when(kafkaAdminService.describeGroups(anyCollection())).thenReturn(described);
    }

    private void topicoRequestsEvents(int partitions) {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events", "audit.timeline"));
        when(kafkaAdminService.topicPartitionCounts(anyCollection()))
                .thenReturn(Map.of("requests.events", partitions));
    }

    // ---------- listado ----------

    @Test
    void elListadoIncluyeGruposDelBrokerYLosDelSistema() {
        brokerTieneGrupos(
                Set.of("report-group", "otro-grupo"),
                Map.of(
                        "report-group", new KafkaAdminService.GroupDescription("STABLE", 1),
                        "otro-grupo", new KafkaAdminService.GroupDescription("EMPTY", 0)));

        List<GroupSummaryView> out = service.listGroups();

        assertThat(out).extracting(GroupSummaryView::groupId)
                .containsExactly("report-group", "audit-group", "otro-grupo");
        assertThat(out.get(0).system()).isTrue();
        assertThat(out.get(0).state()).isEqualTo("STABLE");
        assertThat(out.get(0).members()).isEqualTo(1);
        assertThat(out.get(1).system()).isTrue();
        assertThat(out.get(1).state()).isEqualTo("DEAD");
        assertThat(out.get(1).members()).isZero();
        assertThat(out.get(2).system()).isFalse();
    }

    // ---------- detalle y lag ----------

    @Test
    void elDetalleCalculaLagDesdeElOffsetConfirmado() {
        brokerTieneGrupos(Set.of("report-group"), Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 1)));
        when(kafkaAdminService.committedOffsets("report-group"))
                .thenReturn(Map.of(new TopicPartition("requests.events", 0), 100L, new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events", "audit.timeline"));
        when(kafkaAdminService.topicPartitionCounts(anyCollection())).thenReturn(Map.of("requests.events", 2));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 108L,
                new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.earliestOffsets(anyCollection())).thenReturn(Map.of());

        GroupDetailView detail = service.describeGroup("report-group");

        assertThat(detail.system()).isTrue();
        assertThat(detail.members()).isEqualTo(1);
        assertThat(detail.topics()).hasSize(1);
        var p0 = detail.topics().get(0).partitions().get(0);
        assertThat(p0.committedOffset()).isEqualTo(100L);
        assertThat(p0.endOffset()).isEqualTo(108L);
        assertThat(p0.lag()).isEqualTo(8L);
        assertThat(p0.lagBasis()).isEqualTo("COMMITTED");
    }

    @Test
    void grupoDelSistemaSinOffsetMideLagDesdeElEarliest() {
        brokerTieneGrupos(Set.of("audit-group"), Map.of("audit-group", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.committedOffsets("audit-group")).thenReturn(Map.of());
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events", "audit.timeline"));
        when(kafkaAdminService.topicPartitionCounts(anyCollection()))
                .thenReturn(Map.of("requests.events", 1, "audit.timeline", 1));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 20L,
                new TopicPartition("audit.timeline", 0), 15L));
        when(kafkaAdminService.earliestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 10L,
                new TopicPartition("audit.timeline", 0), 5L));

        GroupDetailView detail = service.describeGroup("audit-group");

        assertThat(detail.topics()).hasSize(2);
        var p = detail.topics().get(0).partitions().get(0);
        assertThat(p.committedOffset()).isNull();
        assertThat(p.lag()).isEqualTo(10L);
        assertThat(p.lagBasis()).isEqualTo("EARLIEST");
    }

    @Test
    void otroGrupoSinOffsetEnUnaParticionDejaLagEnNull() {
        brokerTieneGrupos(Set.of("raro"), Map.of("raro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.committedOffsets("raro"))
                .thenReturn(Map.of(new TopicPartition("requests.events", 0), 10L));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events"));
        when(kafkaAdminService.topicPartitionCounts(anyCollection())).thenReturn(Map.of("requests.events", 2));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 15L,
                new TopicPartition("requests.events", 1), 20L));

        GroupDetailView detail = service.describeGroup("raro");

        assertThat(detail.topics()).hasSize(1);
        var rows = detail.topics().get(0).partitions();
        assertThat(rows.get(0).lag()).isEqualTo(5L);
        assertThat(rows.get(0).lagBasis()).isEqualTo("COMMITTED");
        assertThat(rows.get(1).committedOffset()).isNull();
        assertThat(rows.get(1).lag()).isNull();
        assertThat(rows.get(1).lagBasis()).isNull();
    }

    @Test
    void grupoInexistenteQueNoEsDelSistemaDa404() {
        brokerTieneGrupos(Set.of("report-group"), Map.of());

        assertThatThrownBy(() -> service.describeGroup("fantasma"))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
    }

    // ---------- validaciones del reset ----------

    private ResetOffsetsRequest pedido(String topic, String strategy, Instant ts, Boolean dryRun) {
        return new ResetOffsetsRequest(topic, strategy, ts, dryRun);
    }

    @Test
    void resetValidaTopicDryRunEstrategiaYTtimestamp() {
        assertBadRequest(pedido(null, "EARLIEST", null, true), "topic es obligatorio");
        assertBadRequest(pedido("  ", "EARLIEST", null, true), "topic es obligatorio");
        assertBadRequest(pedido("requests.events", "EARLIEST", null, null), "dryRun es obligatorio");
        assertBadRequest(pedido("requests.events", "RANDOM", null, true), "La estrategia debe ser EARLIEST, LATEST o TIMESTAMP");
        assertBadRequest(pedido("requests.events", "TIMESTAMP", null, true), "timestamp es obligatorio con TIMESTAMP");
        assertBadRequest(pedido("requests.events", "TIMESTAMP", Instant.now().plus(1, ChronoUnit.HOURS), true),
                "timestamp no puede ser futuro");
    }

    private void assertBadRequest(ResetOffsetsRequest req, String message) {
        assertThatThrownBy(() -> service.resetOffsets("report-group", req))
                .isInstanceOfSatisfying(ResponseStatusException.class, e -> {
                    assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(e.getReason()).isEqualTo(message);
                });
    }

    // ---------- reset: errores de dominio ----------

    @Test
    void resetGrupoInexistenteDa404() {
        brokerTieneGrupos(Set.of(), Map.of());
        assertThatThrownBy(() -> service.resetOffsets("fantasma", pedido("requests.events", "EARLIEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND));
    }

    @Test
    void resetTopicoInexistenteDa404() {
        brokerTieneGrupos(Set.of("otro"), Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events"));
        when(kafkaAdminService.describeGroups(Set.of("otro"))).thenReturn(Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        assertThatThrownBy(() -> service.resetOffsets("otro", pedido("no.existe", "EARLIEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> assertThat(e.getReason()).isEqualTo("El topico no existe"));
    }

    @Test
    void resetConMiembrosActivosDa409() {
        brokerTieneGrupos(Set.of("report-group"), Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 2)));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events"));
        when(kafkaAdminService.describeGroups(Set.of("report-group")))
                .thenReturn(Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 2)));
        assertThatThrownBy(() -> service.resetOffsets("report-group", pedido("requests.events", "EARLIEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> {
                            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                            assertThat(e.getReason()).isEqualTo("El grupo tiene miembros activos");
                        });
    }

    @Test
    void resetDeGrupoDelSistemaFueraDeSusTopicosDa409() {
        brokerTieneGrupos(Set.of("report-group"), Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.describeGroups(Set.of("report-group")))
                .thenReturn(Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events", "audit.timeline"));
        assertThatThrownBy(() -> service.resetOffsets("report-group", pedido("audit.timeline", "EARLIEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> {
                            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                            assertThat(e.getReason()).isEqualTo("El grupo no consume ese topico");
                        });
    }

    @Test
    void resetDeOtroGrupoSinOffsetsConfirmadosDa409() {
        brokerTieneGrupos(Set.of("otro"), Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.describeGroups(Set.of("otro")))
                .thenReturn(Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events"));
        when(kafkaAdminService.committedOffsets("otro")).thenReturn(Map.of());
        assertThatThrownBy(() -> service.resetOffsets("otro", pedido("requests.events", "EARLIEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        e -> {
                            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                            assertThat(e.getReason()).isEqualTo("El grupo no tiene offsets en ese topico");
                        });
    }

    // ---------- restricciones de grupos del sistema ----------

    private void grupoSistemaSinMiembros() {
        brokerTieneGrupos(Set.of("report-group"), Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.describeGroups(Set.of("report-group")))
                .thenReturn(Map.of("report-group", new KafkaAdminService.GroupDescription("STABLE", 0)));
        topicoRequestsEvents(2);
    }

    @Test
    void latestEnGrupoDelSistemaSiempreEs409() {
        grupoSistemaSinMiembros();
        when(kafkaAdminService.committedOffsets("report-group")).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 100L,
                new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 120L,
                new TopicPartition("requests.events", 1), 60L));

        assertThatThrownBy(() -> service.resetOffsets("report-group", pedido("requests.events", "LATEST", null, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class, e -> {
                    assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(e.getReason()).isEqualTo("El reset adelantaria offsets en las particiones 0, 1");
                });
    }

    @Test
    void timestampQueSuperaElConfirmadoEs409EnGrupoDelSistema() {
        grupoSistemaSinMiembros();
        when(kafkaAdminService.committedOffsets("report-group")).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 100L,
                new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 200L,
                new TopicPartition("requests.events", 1), 80L));
        when(kafkaAdminService.offsetsAtTimestamp(anyCollection(), any(Instant.class))).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 150L,
                new TopicPartition("requests.events", 1), 40L));

        Instant ts = Instant.now().minus(1, ChronoUnit.DAYS);
        assertThatThrownBy(() -> service.resetOffsets("report-group", pedido("requests.events", "TIMESTAMP", ts, true)))
                .isInstanceOfSatisfying(ResponseStatusException.class, e -> {
                    assertThat(e.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(e.getReason()).isEqualTo("El reset adelantaria offsets en las particiones 0");
                });
    }

    @Test
    void timestampSinOffsetConfirmadoEs409EnGrupoDelSistema() {
        grupoSistemaSinMiembros();
        when(kafkaAdminService.committedOffsets("report-group")).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 100L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 200L,
                new TopicPartition("requests.events", 1), 80L));
        when(kafkaAdminService.offsetsAtTimestamp(anyCollection(), any(Instant.class))).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 50L,
                new TopicPartition("requests.events", 1), 10L));

        assertThatThrownBy(() -> service.resetOffsets("report-group",
                        pedido("requests.events", "TIMESTAMP", Instant.now().minus(1, ChronoUnit.DAYS), true)))
                .isInstanceOfSatisfying(ResponseStatusException.class, e ->
                        assertThat(e.getReason()).isEqualTo("El reset adelantaria offsets en las particiones 1"));
    }

    // ---------- dry run y reset real ----------

    @Test
    void earliestHaciaAtrasEsPermitidoYElDryRunNoEscribe() {
        grupoSistemaSinMiembros();
        when(kafkaAdminService.committedOffsets("report-group")).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 100L,
                new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 120L,
                new TopicPartition("requests.events", 1), 60L));
        when(kafkaAdminService.earliestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 10L,
                new TopicPartition("requests.events", 1), 5L));

        ResetOffsetsResponse out = service.resetOffsets("report-group", pedido("requests.events", "EARLIEST", null, true));

        assertThat(out.dryRun()).isTrue();
        assertThat(out.partitions()).hasSize(2);
        assertThat(out.partitions().get(0).currentOffset()).isEqualTo(100L);
        assertThat(out.partitions().get(0).targetOffset()).isEqualTo(10L);
        assertThat(out.partitions().get(0).lagAfter()).isEqualTo(110L);
        verify(kafkaAdminService, never()).alterGroupOffsets(anyString(), any());
    }

    @Test
    void resetRealEscribeLosOffsets() {
        grupoSistemaSinMiembros();
        when(kafkaAdminService.committedOffsets("report-group")).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 100L,
                new TopicPartition("requests.events", 1), 50L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 120L,
                new TopicPartition("requests.events", 1), 60L));
        when(kafkaAdminService.earliestOffsets(anyCollection())).thenReturn(Map.of(
                new TopicPartition("requests.events", 0), 10L,
                new TopicPartition("requests.events", 1), 5L));

        ResetOffsetsResponse out = service.resetOffsets("report-group", pedido("requests.events", "earliest", null, false));

        assertThat(out.dryRun()).isFalse();
        assertThat(out.strategy()).isEqualTo("EARLIEST");
        verify(kafkaAdminService).alterGroupOffsets(eq("report-group"), any());
    }

    @Test
    void otroGrupoPermiteLatest() {
        brokerTieneGrupos(Set.of("otro"), Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.describeGroups(Set.of("otro")))
                .thenReturn(Map.of("otro", new KafkaAdminService.GroupDescription("STABLE", 0)));
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events"));
        when(kafkaAdminService.topicPartitionCounts(anyCollection())).thenReturn(Map.of("requests.events", 1));
        when(kafkaAdminService.committedOffsets("otro")).thenReturn(Map.of(new TopicPartition("requests.events", 0), 10L));
        when(kafkaAdminService.latestOffsets(anyCollection())).thenReturn(Map.of(new TopicPartition("requests.events", 0), 30L));

        ResetOffsetsResponse out = service.resetOffsets("otro", pedido("requests.events", "LATEST", null, true));

        assertThat(out.partitions().get(0).targetOffset()).isEqualTo(30L);
        assertThat(out.partitions().get(0).lagAfter()).isEqualTo(0L);
    }
}
