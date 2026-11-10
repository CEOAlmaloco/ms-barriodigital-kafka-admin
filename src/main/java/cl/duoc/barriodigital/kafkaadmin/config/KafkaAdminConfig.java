package cl.duoc.barriodigital.kafkaadmin.config;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans de infraestructura de Kafka del servicio. */
@Configuration
public class KafkaAdminConfig {

    private static final String ADMIN_API_TIMEOUT_MS = "5000";

    /**
     * Cliente de administración. Solo kafka-admin lo usa: los demás servicios
     * no crean tópicos (auto.create.topics.enable=false en el broker).
     * {@code default.api.timeout.ms=5s} porque el default de 60 s colgaría el BFF.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public AdminClient kafkaAdminClient(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildAdminProperties());
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, ADMIN_API_TIMEOUT_MS);
        props.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, ADMIN_API_TIMEOUT_MS);
        return AdminClient.create(props);
    }

    /** Reloj UTC para los topes de tiempo del replay de DLT. */
    @Bean
    @ConditionalOnMissingBean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
