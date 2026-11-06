package cl.duoc.barriodigital.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Arranca kafka-admin.
 * Deja listo el proceso que administra topics y consumer groups de Kafka.
 * Escucha en el puerto 8085, que no es público.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaAdminApplication {

    /**
     * Punto de entrada. Spring Boot inicia el proceso y abre el puerto 8085.
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
