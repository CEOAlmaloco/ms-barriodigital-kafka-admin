package cl.duoc.barriodigital.kafkaadmin.messaging;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

/** Productor de audit.timeline: acks=all, idempotencia, sin NewTopic. */
@Component
public class DefaultAuditKafkaSender implements AuditKafkaSender {

    private static final long WAIT_MS = 10_000L;

    private final KafkaProperties kafkaProperties;

    public DefaultAuditKafkaSender(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void sendConfirmed(String topic, String key, String json) throws Exception {
        Properties props = producerProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, json));
            future.get(WAIT_MS, TimeUnit.MILLISECONDS);
            producer.flush();
        }
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaProperties.getBootstrapServers()));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
