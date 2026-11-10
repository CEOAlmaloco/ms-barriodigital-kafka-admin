package cl.duoc.barriodigital.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.barriodigital.kafkaadmin.config.InternalTokenProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaGroupsProperties;
import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaRecordClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(properties = {
        "barriodigital.security.internal-token=test-token",
        "management.health.kafka.enabled=false"
})
class KafkaAdminApplicationTest {

    @Autowired
    private KafkaTopicsProperties topicsProperties;

    @Autowired
    private KafkaGroupsProperties groupsProperties;

    @Autowired
    private InternalTokenProperties tokenProperties;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private KafkaRecordClient recordClient;

    @Test
    void elContextoArranca() {
        assertThat(tokenProperties.getInternalToken()).isEqualTo("test-token");
    }

    @Test
    void losTopicosDelSistemaQuedanDeclaradosEnConfiguracion() {
        assertThat(topicsProperties.getTopics()).hasSize(5);
        assertThat(topicsProperties.getTopics().get("requests.events").getPartitions()).isEqualTo(3);
        assertThat(topicsProperties.getTopics().get("audit.timeline").getCleanupPolicy()).isEqualTo("compact,delete");
        assertThat(topicsProperties.getTopics().get("requests.events.report.DLT").getMessageTimestampType())
                .isEqualTo("LogAppendTime");
        assertThat(topicsProperties.getTopics().get("audit.timeline.audit.DLT").getRetentionMs())
                .isEqualTo(1209600000L);
        assertThat(groupsProperties.getGroups()).containsOnlyKeys("report-group", "audit-group");
        assertThat(groupsProperties.getGroups().get("audit-group").getTopics())
                .containsExactly("requests.events", "audit.timeline");
    }
}
