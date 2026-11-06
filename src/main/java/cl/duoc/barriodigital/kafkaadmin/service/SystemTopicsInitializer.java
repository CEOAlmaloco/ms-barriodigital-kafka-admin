package cl.duoc.barriodigital.kafkaadmin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Al arrancar crea los tópicos del sistema que falten. Si Kafka no responde,
 * el servicio sigue arriba y reintenta cada {@code barriodigital.kafka.topics-init.retry-ms}
 * milisegundos hasta lograrlo.
 */
@Component
public class SystemTopicsInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SystemTopicsInitializer.class);

    private final KafkaAdminService kafkaAdminService;
    private final long retryMs;
    private volatile boolean initialized;

    public SystemTopicsInitializer(
            KafkaAdminService kafkaAdminService,
            @Value("${barriodigital.kafka.topics-init.retry-ms:10000}") long retryMs) {
        this.kafkaAdminService = kafkaAdminService;
        this.retryMs = retryMs;
    }

    @Override
    public void run(ApplicationArguments args) {
        attempt();
    }

    /** Reintenta la creación inicial mientras no se haya completado. */
    @Scheduled(fixedDelayString = "${barriodigital.kafka.topics-init.retry-ms:10000}", initialDelayString = "${barriodigital.kafka.topics-init.retry-ms:10000}")
    public void retry() {
        if (!initialized) {
            attempt();
        }
    }

    private void attempt() {
        try {
            kafkaAdminService.ensureSystemTopics();
            initialized = true;
        } catch (Exception e) {
            log.warn("No se pudieron crear los topicos del sistema (reintento en {} ms): {}", retryMs, e.getMessage());
        }
    }
}
