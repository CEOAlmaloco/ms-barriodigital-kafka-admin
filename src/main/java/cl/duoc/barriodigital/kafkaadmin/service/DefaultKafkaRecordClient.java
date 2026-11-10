package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

/** Consumidor asignado y productor con ACK all, usados solo por el listado y el replay de DLT. */
@Component
public class DefaultKafkaRecordClient implements KafkaRecordClient {

    private static final Duration POLL = Duration.ofMillis(250);

    private final KafkaProperties kafkaProperties;

    public DefaultKafkaRecordClient(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public List<ConsumedRecord> readAssigned(
            String topic,
            Map<Integer, Long> fromOffsetsByPartition,
            int limitPerPartition,
            Duration timeout
    ) {
        if (fromOffsetsByPartition.isEmpty() || limitPerPartition < 1) {
            return List.of();
        }
        Properties props = consumerProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> assigned = new ArrayList<>();
            fromOffsetsByPartition.keySet().forEach(p -> assigned.add(new TopicPartition(topic, p)));
            consumer.assign(assigned);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assigned);
            Map<Integer, Integer> remaining = new HashMap<>();
            for (Map.Entry<Integer, Long> entry : fromOffsetsByPartition.entrySet()) {
                TopicPartition tp = new TopicPartition(topic, entry.getKey());
                consumer.seek(tp, entry.getValue());
                remaining.put(entry.getKey(), limitPerPartition);
            }
            List<ConsumedRecord> out = new ArrayList<>();
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline) && remaining.values().stream().anyMatch(n -> n > 0)) {
                for (TopicPartition tp : assigned) {
                    Integer left = remaining.get(tp.partition());
                    long end = endOffsets.getOrDefault(tp, 0L);
                    if (left != null && left > 0 && consumer.position(tp) >= end) {
                        remaining.put(tp.partition(), 0);
                    }
                }
                if (remaining.values().stream().noneMatch(n -> n > 0)) {
                    break;
                }
                long millisLeft = Duration.between(Instant.now(), deadline).toMillis();
                if (millisLeft <= 0) {
                    break;
                }
                ConsumerRecords<String, String> batch = consumer.poll(POLL.compareTo(Duration.ofMillis(millisLeft)) < 0
                        ? POLL
                        : Duration.ofMillis(millisLeft));
                if (batch.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : batch) {
                    Integer left = remaining.get(record.partition());
                    if (left == null || left <= 0) {
                        continue;
                    }
                    out.add(new ConsumedRecord(
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value(),
                            Instant.ofEpochMilli(record.timestamp()),
                            record.headers()
                    ));
                    remaining.put(record.partition(), left - 1);
                }
            }
            out.sort((a, b) -> {
                int byPartition = Integer.compare(a.partition(), b.partition());
                return byPartition != 0 ? byPartition : Long.compare(a.offset(), b.offset());
            });
            return out;
        } catch (Exception e) {
            throw new KafkaUnavailableException("Kafka no esta disponible", e);
        }
    }

    @Override
    public void sendConfirmed(
            String topic,
            int partition,
            String key,
            String value,
            Instant recordTimestamp,
            Headers headers,
            Duration wait
    ) throws Exception {
        Properties props = producerProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    partition,
                    recordTimestamp.toEpochMilli(),
                    key,
                    value,
                    headers
            );
            Future<RecordMetadata> future = producer.send(record);
            future.get(wait.toMillis(), TimeUnit.MILLISECONDS);
            producer.flush();
        }
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaProperties.getBootstrapServers()));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return props;
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaProperties.getBootstrapServers()));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        return props;
    }
}
