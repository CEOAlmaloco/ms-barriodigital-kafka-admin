package cl.duoc.barriodigital.kafkaadmin.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AuditPublisherTest {

    private AuditKafkaSender sender;
    private AuditPublisher publisher;
    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        sender = mock(AuditKafkaSender.class);
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        publisher = new AuditPublisher(sender, mapper);
        logger = (Logger) LoggerFactory.getLogger(AuditPublisher.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        publisher.shutdown();
    }

    @Test
    void envioFallidoRegistraEventoCompletoEnElLog() throws Exception {
        doThrow(new RuntimeException("Topic audit.timeline not present in metadata after 5000 ms."))
                .when(sender).sendConfirmed(anyString(), anyString(), anyString());

        AuditEnvelope event = sample("topic.created");
        publisher.publish(event);

        verify(sender, timeout(3000)).sendConfirmed(eq(AuditPublisher.TOPIC), eq("topic:demo.events"), anyString());
        awaitLog(AuditPublisher.LOG_MARKER);
        ILoggingEvent entry = appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains(AuditPublisher.LOG_MARKER))
                .findFirst()
                .orElseThrow();
        assertThat(entry.getLevel()).isEqualTo(Level.ERROR);
        assertThat(entry.getFormattedMessage()).contains("motivo=");
        assertThat(entry.getFormattedMessage()).contains("evento=");
        assertThat(entry.getFormattedMessage()).contains(event.eventId().toString());
        assertThat(entry.getFormattedMessage()).contains("\"occurredAt\":\"2026-11-12T15:00:00Z\"");
    }

    @Test
    void apagadoVaciaLaColaConMotivoApagadoDelServicio() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(inv -> {
            entered.countDown();
            release.await(30, TimeUnit.SECONDS);
            return null;
        }).when(sender).sendConfirmed(anyString(), anyString(), anyString());

        publisher.publish(sample("topic.created"));
        assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
        publisher.publish(sample("topic.deleted"));

        publisher.shutdown();
        release.countDown();

        awaitLog(AuditPublisher.MOTIVO_APAGADO);
        assertThat(appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains(AuditPublisher.LOG_MARKER))
                .filter(m -> m.contains(AuditPublisher.MOTIVO_APAGADO))
                .count()).isGreaterThanOrEqualTo(1);
        assertThat(publisher.queueSize()).isZero();
    }

    private void awaitLog(String fragment) throws InterruptedException {
        for (int i = 0; i < 80; i++) {
            boolean found = appender.list.stream()
                    .anyMatch(e -> e.getFormattedMessage().contains(fragment));
            if (found) {
                return;
            }
            Thread.sleep(50);
        }
    }

    private static AuditEnvelope sample(String eventType) {
        String action = eventType.substring(eventType.indexOf('.') + 1);
        return new AuditEnvelope(
                UUID.randomUUID(),
                eventType,
                1,
                Instant.parse("2026-11-12T15:00:00Z"),
                "kafka-admin",
                UUID.randomUUID(),
                new AuditActor("u1", "Admin Test", "Admin", "127.0.0.1"),
                new AuditPayload("topic", "demo.events", action, null, Map.of("partitions", 3))
        );
    }
}
