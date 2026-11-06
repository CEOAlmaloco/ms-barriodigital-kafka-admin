package cl.duoc.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties.TopicSpec;
import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class KafkaAdminServiceTest {

    @Mock
    private AdminClient admin;

    private KafkaAdminService service;
    private KafkaTopicsProperties properties;

    @BeforeEach
    void setUp() {
        properties = new KafkaTopicsProperties();
        Map<String, TopicSpec> topics = new HashMap<>();
        topics.put("requests.events", spec(3, 3, "delete", "604800000", "2", null, null));
        topics.put("audit.timeline", spec(3, 3, "compact,delete", "2592000000", "2", "604800000", null));
        topics.put("requests.events.report.DLT", spec(3, 3, "delete", "1209600000", "2", null, "LogAppendTime"));
        topics.put("requests.events.audit.DLT", spec(3, 3, "delete", "1209600000", "2", null, "LogAppendTime"));
        topics.put("audit.timeline.audit.DLT", spec(3, 3, "delete", "1209600000", "2", null, "LogAppendTime"));
        properties.setTopics(topics);
        service = new KafkaAdminService(admin, properties);
    }

    private TopicSpec spec(Integer partitions, Integer rf, String cleanup, String retention,
            String minIsr, String minCompactionLag, String timestampType) {
        TopicSpec spec = new TopicSpec();
        spec.setPartitions(partitions);
        spec.setReplicationFactor(rf);
        spec.setCleanupPolicy(cleanup);
        spec.setRetentionMs(Long.parseLong(retention));
        spec.setMinInsyncReplicas(Integer.parseInt(minIsr));
        if (minCompactionLag != null) {
            spec.setMinCompactionLagMs(Long.parseLong(minCompactionLag));
        }
        spec.setMessageTimestampType(timestampType);
        return spec;
    }

    private void kafkaDisponible() throws Exception {
        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("cluster-test"));
        when(admin.describeCluster()).thenReturn(cluster);
    }

    private void topicosExistentes(Set<String> names) throws Exception {
        ListTopicsResult listing = mock(ListTopicsResult.class);
        when(listing.names()).thenReturn(KafkaFuture.completedFuture(names));
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(listing);
    }

    private void createTopicsOk() throws Exception {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection(), any(CreateTopicsOptions.class))).thenReturn(result);
    }

    @Test
    void creaSoloLosTopicosFaltantesConLosParametrosDelSistema() throws Exception {
        kafkaDisponible();
        topicosExistentes(Set.of("requests.events", "audit.timeline"));
        createTopicsOk();

        service.ensureSystemTopics();

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture(), any(CreateTopicsOptions.class));
        Collection<NewTopic> created = captor.getValue();
        assertThat(created).hasSize(3);
        NewTopic dlt = created.stream()
                .filter(t -> t.name().equals("requests.events.report.DLT"))
                .findFirst().orElseThrow();
        assertThat(dlt.numPartitions()).isEqualTo(3);
        assertThat(dlt.replicationFactor()).isEqualTo((short) 3);
        assertThat(dlt.configs())
                .containsEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
                .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "1209600000")
                .containsEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }

    @Test
    void noCreaNiModificaCuandoTodosExisten() throws Exception {
        kafkaDisponible();
        topicosExistentes(Set.copyOf(properties.getTopics().keySet()));
        configsConDefaults();

        service.ensureSystemTopics();

        verify(admin, never()).createTopics(anyCollection(), any(CreateTopicsOptions.class));
        verify(admin, never()).incrementalAlterConfigs(org.mockito.ArgumentMatchers.<Map<ConfigResource, java.util.Collection<org.apache.kafka.clients.admin.AlterConfigOp>>>any(), any());
        verify(admin, never()).alterConfigs(org.mockito.ArgumentMatchers.<Map<ConfigResource, Config>>any(), any());
    }

    @Test
    void avisaSinModificarCuandoUnTopicoExistenteDifere(CapturedOutput output) throws Exception {
        kafkaDisponible();
        topicosExistentes(Set.copyOf(properties.getTopics().keySet()));

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "requests.events");
        Config config = new Config(List.of(
                new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "999")));
        DescribeConfigsResult described = mock(DescribeConfigsResult.class);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        when(admin.describeConfigs(anyCollection(), any(DescribeConfigsOptions.class))).thenReturn(described);

        service.ensureSystemTopics();

        assertThat(output).contains("requests.events").contains("retention.ms");
        verify(admin, never()).createTopics(anyCollection(), any(CreateTopicsOptions.class));
        verify(admin, never()).incrementalAlterConfigs(org.mockito.ArgumentMatchers.<Map<ConfigResource, java.util.Collection<org.apache.kafka.clients.admin.AlterConfigOp>>>any(), any());
        verify(admin, never()).alterConfigs(org.mockito.ArgumentMatchers.<Map<ConfigResource, Config>>any(), any());
    }

    @Test
    void lanzaKafkaUnavailableCuandoElClusterNoResponde() {
        when(admin.describeCluster()).thenThrow(new RuntimeException("sin brokers"));

        assertThatThrownBy(() -> service.ensureSystemTopics())
                .isInstanceOf(KafkaUnavailableException.class);
    }

    @Test
    void isAvailableReflejaLaRespuestaDelCluster() throws Exception {
        kafkaDisponible();
        assertThat(service.isAvailable()).isTrue();

        when(admin.describeCluster()).thenThrow(new RuntimeException("timeout"));
        assertThat(service.isAvailable()).isFalse();
    }

    private void configsConDefaults() throws Exception {
        List<ConfigResource> resources = new ArrayList<>();
        Map<ConfigResource, Config> values = new HashMap<>();
        for (String name : properties.getTopics().keySet()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
            resources.add(resource);
            List<ConfigEntry> entries = new ArrayList<>();
            for (Map.Entry<String, String> wanted : properties.getTopics().get(name).toConfigs().entrySet()) {
                entries.add(new ConfigEntry(wanted.getKey(), wanted.getValue()));
            }
            values.put(resource, new Config(entries));
        }
        DescribeConfigsResult described = mock(DescribeConfigsResult.class);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(values));
        when(admin.describeConfigs(anyCollection(), any(DescribeConfigsOptions.class))).thenReturn(described);
    }
}
