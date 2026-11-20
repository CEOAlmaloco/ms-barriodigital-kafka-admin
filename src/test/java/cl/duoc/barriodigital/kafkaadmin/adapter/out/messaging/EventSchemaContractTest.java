package cl.duoc.barriodigital.kafkaadmin.adapter.out.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * El JSON que kafka-admin publica en audit.timeline cumple los esquemas de contracts/events, uno por cada
 * combinación de recurso y acción de kafka-topicos.md, sección 7. Un cambio de campos obliga a actualizar el esquema
 * y a decidir si sube schemaVersion (sección 11).
 */
class EventSchemaContractTest {

    private static final Path EVENTS = Path.of("contracts", "events");
    private static final Instant NOW = Instant.parse("2026-11-12T12:45:08.003Z");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    private static final SchemaValidatorsConfig CONFIG = SchemaValidatorsConfig.builder()
            .formatAssertionsEnabled(true)
            .build();

    private final AuditEventFactory events = new AuditEventFactory(Clock.fixed(NOW, ZoneOffset.UTC));

    @AfterEach
    void clearActor() {
        ActorContext.clear();
    }

    @Test
    void cadaAccionAuditadaCumpleSuEsquema() throws Exception {
        ActorContext.set(new AuditActor("2a3b4c5d-6e7f-4a8b-9c0d-1e2f3a4b5c6d", "Admin Test", "Admin", "203.0.113.25"));
        for (Map.Entry<String, AuditEnvelope> entry : samples().entrySet()) {
            JsonNode published = publish(entry.getValue());
            assertThat(schema(entry.getKey()).validate(published))
                    .as(entry.getKey() + " no cumple su esquema")
                    .isEmpty();
        }
    }

    @Test
    void hayUnEsquemaPorCadaAccionDelContratoYNingunoDeMas() throws Exception {
        try (Stream<Path> files = Files.list(EVENTS)) {
            Set<String> schemas = files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".v1.schema.json"))
                    .map(name -> name.replace(".v1.schema.json", ""))
                    .collect(Collectors.toSet());
            assertThat(schemas).isEqualTo(samples().keySet());
        }
    }

    @Test
    void laSequenceDeRequestsOUnBeforeEnUnaCreacionRompenElEsquema() throws Exception {
        ActorContext.set(new AuditActor("u1", "Admin Test", "Admin", null));
        Map.Entry<String, AuditEnvelope> created = samples().entrySet().stream()
                .filter(entry -> entry.getKey().endsWith(".created"))
                .findFirst()
                .orElseThrow();
        ObjectNode event = (ObjectNode) publish(created.getValue());
        JsonSchema schema = schema(created.getKey());

        ObjectNode withSequence = event.deepCopy();
        withSequence.put("sequence", 1);
        ObjectNode withBefore = event.deepCopy();
        ((ObjectNode) withBefore.get("payload")).putObject("before");
        ObjectNode withoutSourceIp = event.deepCopy();
        ((ObjectNode) withoutSourceIp.get("actor")).remove("sourceIp");

        for (ObjectNode broken : List.of(withSequence, withBefore, withoutSourceIp)) {
            assertThat(schema.validate(broken)).as("el esquema aceptó " + broken).isNotEmpty();
        }
    }

    private Map<String, AuditEnvelope> samples() {
        Map<String, AuditEnvelope> samples = new LinkedHashMap<>();
        add(samples, "topic", "demo.avisos", "created", null, Map.of("name", "demo.avisos", "partitions", 3));
        add(samples, "topic", "demo.avisos", "updated", Map.of("retention.ms", "604800000"),
                Map.of("retention.ms", "259200000"));
        add(samples, "topic", "demo.avisos", "deleted", Map.of("name", "demo.avisos", "partitions", 3), null);
        add(samples, "topic", "requests.events.report.DLT", "dlt-replayed", null,
                Map.of("replayed", List.of(Map.of("eventId", "3c9e2f10-7a4b-4c1d-9e8f-0a1b2c3d4e5f", "result", "OK"))));
        add(samples, "consumer-group", "report", "offsets-reset", Map.of("requests.events-0", 120),
                Map.of("requests.events-0", 0));
        return samples;
    }

    private void add(Map<String, AuditEnvelope> samples, String resourceType, String resourceId, String action,
                     Object before, Object after) {
        samples.put(resourceType + "." + action, events.create(resourceType, resourceId, action, before, after));
    }

    /** El JSON tal como AuditPublisher lo envía a Kafka. */
    private static JsonNode publish(AuditEnvelope envelope) throws Exception {
        AuditKafkaSender sender = mock(AuditKafkaSender.class);
        AuditPublisher publisher = new AuditPublisher(sender, new ObjectMapper());
        try {
            publisher.publish(envelope);
            ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
            verify(sender, timeout(5_000)).sendConfirmed(eq(AuditPublisher.TOPIC), anyString(), json.capture());
            return JSON.readTree(json.getValue());
        } finally {
            publisher.shutdown();
        }
    }

    private static JsonSchema schema(String eventType) {
        Path file = EVENTS.resolve(eventType + ".v1.schema.json");
        return FACTORY.getSchema(SchemaLocation.of(file.toAbsolutePath().toUri().toString()), CONFIG);
    }
}
