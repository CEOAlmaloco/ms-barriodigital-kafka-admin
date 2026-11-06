package cl.duoc.barriodigital.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans de infraestructura de Kafka del servicio. */
@Configuration
public class KafkaAdminConfig {

    /**
     * Cliente de administración. Solo kafka-admin lo usa: los demás servicios
     * no crean tópicos (auto.create.topics.enable=false en el broker).
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public AdminClient kafkaAdminClient(KafkaProperties kafkaProperties) {
        return AdminClient.create(kafkaProperties.buildAdminProperties());
    }
}
