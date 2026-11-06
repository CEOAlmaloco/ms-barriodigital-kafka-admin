package cl.duoc.barriodigital.kafkaadmin.domain;

/** Kafka no respondió a tiempo. Los endpoints que lo necesitan responden 503. */
public class KafkaUnavailableException extends RuntimeException {

    public KafkaUnavailableException(String message) {
        super(message);
    }

    public KafkaUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
