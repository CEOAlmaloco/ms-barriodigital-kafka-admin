package cl.duoc.barriodigital.kafkaadmin.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AuditEventFactoryTest {

    private static final Instant NOW = Instant.parse("2026-11-12T15:00:00Z");

    @AfterEach
    void clear() {
        ActorContext.clear();
    }

    @Test
    void armaSobreConKeyActorYPayloadDelContrato() {
        ActorContext.set(new AuditActor(
                "2a3b4c5d-6e7f-4a8b-9c0d-1e2f3a4b5c6d",
                "Admin Test",
                "Admin",
                "203.0.113.25"));
        AuditEventFactory factory = new AuditEventFactory(Clock.fixed(NOW, ZoneOffset.UTC));

        AuditEnvelope event = factory.create(
                "topic",
                "demo.events",
                "updated",
                Map.of("partitions", 3),
                Map.of("partitions", 6));

        assertThat(event.eventType()).isEqualTo("topic.updated");
        assertThat(event.schemaVersion()).isEqualTo(1);
        assertThat(event.occurredAt()).isEqualTo(NOW);
        assertThat(event.producer()).isEqualTo("kafka-admin");
        assertThat(event.key()).isEqualTo("topic:demo.events");
        assertThat(event.actor().id()).isEqualTo("2a3b4c5d-6e7f-4a8b-9c0d-1e2f3a4b5c6d");
        assertThat(event.actor().name()).isEqualTo("Admin Test");
        assertThat(event.actor().role()).isEqualTo("Admin");
        assertThat(event.actor().sourceIp()).isEqualTo("203.0.113.25");
        assertThat(event.payload().resourceType()).isEqualTo("topic");
        assertThat(event.payload().resourceId()).isEqualTo("demo.events");
        assertThat(event.payload().action()).isEqualTo("updated");
        assertThat(event.payload().before()).isEqualTo(Map.of("partitions", 3));
        assertThat(event.payload().after()).isEqualTo(Map.of("partitions", 6));
        assertThat(event.eventId()).isNotNull();
        assertThat(event.traceId()).isNotNull();
    }

    @Test
    void dltReplayLlevaBeforeNullYEstadosEnAfter() {
        ActorContext.set(new AuditActor("u1", "Admin", "Admin", null));
        AuditEventFactory factory = new AuditEventFactory(Clock.fixed(NOW, ZoneOffset.UTC));

        AuditEnvelope event = factory.create(
                "topic",
                "requests.events.report.DLT",
                "dlt-replayed",
                null,
                Map.of("results", java.util.List.of(Map.of("eventId", "e1", "status", "REENVIADO"))));

        assertThat(event.eventType()).isEqualTo("topic.dlt-replayed");
        assertThat(event.key()).isEqualTo("topic:requests.events.report.DLT");
        assertThat(event.payload().before()).isNull();
        assertThat(event.actor().sourceIp()).isNull();
    }
}
