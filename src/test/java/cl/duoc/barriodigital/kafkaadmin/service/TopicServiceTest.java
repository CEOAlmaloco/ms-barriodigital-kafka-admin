package cl.duoc.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties.TopicSpec;
import cl.duoc.barriodigital.kafkaadmin.messaging.AuditEventFactory;
import cl.duoc.barriodigital.kafkaadmin.messaging.AuditPublisher;
import cl.duoc.barriodigital.kafkaadmin.web.CreateTopicRequest;
import cl.duoc.barriodigital.kafkaadmin.web.TopicDetailView;
import cl.duoc.barriodigital.kafkaadmin.web.UpdatePartitionsRequest;
import cl.duoc.barriodigital.kafkaadmin.web.UpdateTopicConfigsRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
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
class TopicServiceTest {

    @Mock
    private KafkaAdminService kafkaAdminService;

    @Mock
    private AuditEventFactory auditEvents;

    @Mock
    private AuditPublisher auditPublisher;

    private TopicService service;
    private KafkaTopicsProperties properties;

    @BeforeEach
    void setUp() {
        properties = new KafkaTopicsProperties();
        Map<String, TopicSpec> topics = new java.util.HashMap<>();
        topics.put("requests.events", spec());
        topics.put("requests.events.report.DLT", spec());
        properties.setTopics(topics);
        service = new TopicService(kafkaAdminService, properties, auditEvents, auditPublisher);
        lenient().when(auditEvents.create(anyString(), anyString(), anyString(), any(), any()))
                .thenAnswer(inv -> null);

        lenient().when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("requests.events", "requests.events.report.DLT"));
        lenient().when(kafkaAdminService.brokerCount()).thenReturn(3);
    }

    private TopicSpec spec() {
        TopicSpec spec = new TopicSpec();
        spec.setPartitions(3);
        spec.setReplicationFactor(3);
        return spec;
    }

    private TopicDescription description(String name, int partitions, int replicas) {
        Node node = new Node(1, "localhost", 9092);
        List<TopicPartitionInfo> infos = new java.util.ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            infos.add(new TopicPartitionInfo(i, node, List.of(node, node, node).subList(0, replicas), List.of(node)));
        }
        return new TopicDescription(name, false, infos, Set.of());
    }

    private void describeReturns(String name, TopicDescription description) {
        when(kafkaAdminService.describeTopics(anyCollection())).thenReturn(Map.of(name, description));
    }

    private void assertStatus(Runnable call, HttpStatus status, String message) {
        assertThatThrownBy(call::run)
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(e -> {
                    ResponseStatusException rse = (ResponseStatusException) e;
                    assertThat(rse.getStatusCode()).isEqualTo(status);
                    assertThat(rse.getReason()).isEqualTo(message);
                });
    }

    // ---------- Listado ----------

    @Test
    void listaMarcaLosTopicosDelSistema() {
        describeReturns("requests.events", description("requests.events", 3, 3));
        describeReturns("pruebas.carga", description("pruebas.carga", 2, 2));
        when(kafkaAdminService.describeTopics(anyCollection())).thenReturn(Map.of(
                "requests.events", description("requests.events", 3, 3),
                "pruebas.carga", description("pruebas.carga", 2, 2)));

        var list = service.listTopics();

        assertThat(list).hasSize(2);
        assertThat(list.get(0).name()).isEqualTo("pruebas.carga");
        assertThat(list.get(0).system()).isFalse();
        assertThat(list.get(0).replicationFactor()).isEqualTo(2);
        assertThat(list.get(1).name()).isEqualTo("requests.events");
        assertThat(list.get(1).system()).isTrue();
    }

    // ---------- Detalle ----------

    @Test
    void detalleDevuelve404SiNoExiste() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of());
        assertStatus(() -> service.describeTopic("no.existe"), HttpStatus.NOT_FOUND, "El topico no existe");
    }

    @Test
    void detalleIncluyeConfigsEditablesYNoDefaultConSourceYEditable() {
        describeReturns("requests.events", description("requests.events", 3, 3));
        when(kafkaAdminService.topicConfigs("requests.events")).thenReturn(List.of(
                new KafkaAdminService.TopicConfigEntry("retention.ms", "604800000", "TOPIC"),
                new KafkaAdminService.TopicConfigEntry("cleanup.policy", "delete", "TOPIC"),
                new KafkaAdminService.TopicConfigEntry("segment.ms", "604800000", "DEFAULT"),
                new KafkaAdminService.TopicConfigEntry("message.timestamp.type", "CreateTime", "DEFAULT"),
                new KafkaAdminService.TopicConfigEntry("segment.bytes", "104857600", "BROKER"),
                new KafkaAdminService.TopicConfigEntry("compression.type", "producer", "DEFAULT")));

        TopicDetailView detail = service.describeTopic("requests.events");

        assertThat(detail.system()).isTrue();
        assertThat(detail.configs()).extracting("name").containsExactly(
                "retention.ms", "cleanup.policy", "segment.ms", "message.timestamp.type", "segment.bytes");
        assertThat(detail.configs().get(0).editable()).isTrue();
        assertThat(detail.configs().get(1).editable()).isFalse();
        assertThat(detail.configs().get(4).source()).isEqualTo("BROKER");
        assertThat(detail.configs().get(4).editable()).isFalse();
        assertThat(detail.partitions()).hasSize(3);
        assertThat(detail.partitions().get(0).leader()).isEqualTo(1);
    }

    // ---------- Alta ----------

    @Test
    void creaUnTopicoValido() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of(), Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 2, 2));
        when(kafkaAdminService.topicConfigs("pruebas.carga")).thenReturn(List.of());

        TopicDetailView detail = service.createTopic(new CreateTopicRequest(
                "pruebas.carga", 2, 2, Map.of("retention.ms", "3600000")));

        assertThat(detail.name()).isEqualTo("pruebas.carga");
        verify(kafkaAdminService).createTopic(eq("pruebas.carga"), eq(2), eq(2),
                eq(Map.of("retention.ms", "3600000")));
    }

    @Test
    void rechazaNombreInvalido() {
        assertStatus(() -> service.createTopic(new CreateTopicRequest("__sys", 1, 1, null)),
                HttpStatus.BAD_REQUEST, "Nombre de topico invalido");
        assertStatus(() -> service.createTopic(new CreateTopicRequest("con espacio", 1, 1, null)),
                HttpStatus.BAD_REQUEST, "Nombre de topico invalido");
        assertStatus(() -> service.createTopic(new CreateTopicRequest(".", 1, 1, null)),
                HttpStatus.BAD_REQUEST, "Nombre de topico invalido");
        verify(kafkaAdminService, never()).createTopic(anyString(), anyInt(), anyInt(), anyMap());
    }

    @Test
    void rechazaParticionesYReplicasFueraDeRango() {
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 0, 1, null)),
                HttpStatus.BAD_REQUEST, "partitions debe ser un entero de 1 a 12");
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 13, 1, null)),
                HttpStatus.BAD_REQUEST, "partitions debe ser un entero de 1 a 12");
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 1, 4, null)),
                HttpStatus.BAD_REQUEST, "replicationFactor debe ser un entero de 1 a 3");
    }

    @Test
    void replicasNoSuperanLosBrokers() {
        when(kafkaAdminService.brokerCount()).thenReturn(2);
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 1, 3, null)),
                HttpStatus.BAD_REQUEST, "replicationFactor no puede superar el numero de brokers del cluster");
    }

    @Test
    void rechazaTopicoExistente() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("ya.existe"));
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ya.existe", 1, 1, null)),
                HttpStatus.CONFLICT, "El topico ya existe");
    }

    @Test
    void rechazaNombreDlt() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of());
        assertStatus(() -> service.createTopic(new CreateTopicRequest("trampa.DLT", 1, 1, null)),
                HttpStatus.CONFLICT, "El nombre esta reservado para el sistema");
    }

    @Test
    void rechazaChoquePuntoGuionBajo() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("a.b"));
        assertStatus(() -> service.createTopic(new CreateTopicRequest("a_b", 1, 1, null)),
                HttpStatus.CONFLICT, "El nombre no esta disponible");
    }

    @Test
    void rechazaConfigFueraDeListaOrango() {
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 1, 1,
                Map.of("segment.bytes", "100"))), HttpStatus.BAD_REQUEST, "La configuracion segment.bytes no es editable");
        assertStatus(() -> service.createTopic(new CreateTopicRequest("ok.name", 1, 1,
                Map.of("retention.ms", "100"))), HttpStatus.BAD_REQUEST, "retention.ms fuera de rango");
    }

    // ---------- PATCH configs ----------

    @Test
    void patchConfigsValidaNullyClaveDesconocida() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 1, 3));

        Map<String, String> withNull = new java.util.HashMap<>();
        withNull.put("retention.ms", null);
        assertStatus(() -> service.updateConfigs("pruebas.carga", new UpdateTopicConfigsRequest(withNull)),
                HttpStatus.BAD_REQUEST, "El valor de retention.ms no puede ser null");
        assertStatus(() -> service.updateConfigs("pruebas.carga",
                new UpdateTopicConfigsRequest(Map.of("foo.bar", "1"))),
                HttpStatus.BAD_REQUEST, "La configuracion foo.bar no es editable");
    }

    @Test
    void patchConfigsEnSistemaSoloPermiteRetention() {
        describeReturns("requests.events", description("requests.events", 3, 3));

        assertStatus(() -> service.updateConfigs("requests.events",
                new UpdateTopicConfigsRequest(Map.of("cleanup.policy", "compact"))),
                HttpStatus.CONFLICT, "La configuracion no es editable en un topico del sistema");
        assertStatus(() -> service.updateConfigs("requests.events",
                new UpdateTopicConfigsRequest(Map.of("retention.ms", "1000"))),
                HttpStatus.BAD_REQUEST, "retention.ms fuera de rango");

        when(kafkaAdminService.topicConfigs("requests.events")).thenReturn(List.of());
        service.updateConfigs("requests.events", new UpdateTopicConfigsRequest(Map.of("retention.ms", "259200000")));
        verify(kafkaAdminService).updateTopicConfigs("requests.events", Map.of("retention.ms", "259200000"));
    }

    @Test
    void patchConfigsAplicaMinInsyncSegúnReplicas() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 1, 3));
        when(kafkaAdminService.topicConfigs("pruebas.carga")).thenReturn(List.of());

        assertStatus(() -> service.updateConfigs("pruebas.carga",
                new UpdateTopicConfigsRequest(Map.of("min.insync.replicas", "3"))),
                HttpStatus.BAD_REQUEST, "min.insync.replicas fuera de rango");

        service.updateConfigs("pruebas.carga",
                new UpdateTopicConfigsRequest(Map.of("min.insync.replicas", "2")));
        verify(kafkaAdminService).updateTopicConfigs("pruebas.carga", Map.of("min.insync.replicas", "2"));
    }

    // ---------- PATCH partitions ----------

    @Test
    void patchPartitionsSoloAumenta() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 2, 2));
        when(kafkaAdminService.topicConfigs("pruebas.carga")).thenReturn(List.of());

        assertStatus(() -> service.updatePartitions("pruebas.carga", new UpdatePartitionsRequest(2)),
                HttpStatus.BAD_REQUEST, "partitions debe ser mayor que 2 y no superar 12");
        assertStatus(() -> service.updatePartitions("pruebas.carga", new UpdatePartitionsRequest(13)),
                HttpStatus.BAD_REQUEST, "partitions debe ser mayor que 2 y no superar 12");

        service.updatePartitions("pruebas.carga", new UpdatePartitionsRequest(4));
        verify(kafkaAdminService).increaseTopicPartitions("pruebas.carga", 4);
    }

    @Test
    void patchPartitionsEnSistemaEs409() {
        describeReturns("requests.events", description("requests.events", 3, 3));
        assertStatus(() -> service.updatePartitions("requests.events", new UpdatePartitionsRequest(6)),
                HttpStatus.CONFLICT, "El topico es del sistema y no admite mas particiones");
    }

    // ---------- DELETE ----------

    @Test
    void deleteRechazaSistemaYConsumidoresActivos() {
        describeReturns("requests.events", description("requests.events", 3, 3));
        assertStatus(() -> service.deleteTopic("requests.events"),
                HttpStatus.CONFLICT, "El topico es del sistema y no se puede eliminar");

        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 1, 1));
        when(kafkaAdminService.topicHasActiveConsumers("pruebas.carga")).thenReturn(true);
        assertStatus(() -> service.deleteTopic("pruebas.carga"),
                HttpStatus.CONFLICT, "El topico tiene consumidores activos");
    }

    @Test
    void deleteOkSinConsumidores() {
        when(kafkaAdminService.listTopicNames()).thenReturn(Set.of("pruebas.carga"));
        describeReturns("pruebas.carga", description("pruebas.carga", 1, 1));
        when(kafkaAdminService.topicHasActiveConsumers("pruebas.carga")).thenReturn(false);

        service.deleteTopic("pruebas.carga");
        verify(kafkaAdminService).deleteTopic("pruebas.carga");
    }
}
