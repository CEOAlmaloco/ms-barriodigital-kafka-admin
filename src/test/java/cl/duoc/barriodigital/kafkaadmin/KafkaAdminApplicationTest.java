package cl.duoc.barriodigital.kafkaadmin;

import static org.assertj.core.api.Assertions.assertThat;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaTopicsProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaAdminApplicationTest {

    @Autowired
    private KafkaTopicsProperties topicsProperties;

    @Test
    void elContextoArranca() {
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
    }
}
