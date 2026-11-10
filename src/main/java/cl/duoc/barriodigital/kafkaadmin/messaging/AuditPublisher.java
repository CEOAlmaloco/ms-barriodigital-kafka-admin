package cl.duoc.barriodigital.kafkaadmin.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Publica en audit.timeline fuera del hilo HTTP (EP3-02, 6):
 * un hilo, cola de 1000, confirmación por mensaje, respaldo en log.
 */
@Component
public class AuditPublisher {

    public static final String TOPIC = "audit.timeline";
    public static final String LOG_MARKER = "evento-auditoria-no-publicado";
    public static final String MOTIVO_APAGADO = "apagado del servicio";
    public static final String MOTIVO_COLA_LLENA = "cola de auditoria llena";

    private static final Logger log = LoggerFactory.getLogger(AuditPublisher.class);
    private static final int QUEUE_CAPACITY = 1000;

    private final AuditKafkaSender sender;
    private final ObjectMapper objectMapper;
    private final BlockingQueue<Pending> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Pending> inFlight = new AtomicReference<>();
    private final ExecutorService worker;

    public AuditPublisher(AuditKafkaSender sender, ObjectMapper objectMapper) {
        this.sender = sender;
        this.objectMapper = objectMapper.copy()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "audit-timeline-publisher");
            t.setDaemon(true);
            return t;
        });
        this.worker.execute(this::drainLoop);
    }

    /** Encola el evento. Si la cola está llena, lo registra y la operación HTTP ya respondió. */
    public void publish(AuditEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        if (!running.get() || !queue.offer(new Pending(envelope))) {
            logUnpublished(envelope, running.get() ? MOTIVO_COLA_LLENA : MOTIVO_APAGADO);
        }
    }

    private void drainLoop() {
        while (running.get() || !queue.isEmpty()) {
            try {
                Pending next = queue.poll(200, TimeUnit.MILLISECONDS);
                if (next == null) {
                    if (!running.get()) {
                        break;
                    }
                    continue;
                }
                inFlight.set(next);
                try {
                    String json = objectMapper.writeValueAsString(next.envelope());
                    sender.sendConfirmed(TOPIC, next.envelope().key(), json);
                } catch (Exception e) {
                    String motivo = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                    logUnpublished(next.envelope(), motivo);
                } finally {
                    inFlight.set(null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Apagado ordenado: deja de aceptar, espera el envío en curso un momento,
     * y vuelca a log lo pendiente y lo in-flight sin confirmación.
     */
    @PreDestroy
    public void shutdown() {
        running.set(false);
        worker.shutdown();
        try {
            if (!worker.awaitTermination(1, TimeUnit.SECONDS)) {
                worker.shutdownNow();
                worker.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            worker.shutdownNow();
        }
        Pending flying = inFlight.getAndSet(null);
        if (flying != null) {
            logUnpublished(flying.envelope(), MOTIVO_APAGADO);
        }
        Pending leftover;
        while ((leftover = queue.poll()) != null) {
            logUnpublished(leftover.envelope(), MOTIVO_APAGADO);
        }
    }

    void logUnpublished(AuditEnvelope envelope, String motivo) {
        try {
            String json = objectMapper.writeValueAsString(envelope);
            log.error(
                    "{} eventType={} eventId={} motivo=\"{}\" evento={}",
                    LOG_MARKER,
                    envelope.eventType(),
                    envelope.eventId(),
                    motivo,
                    json
            );
        } catch (Exception e) {
            log.error("{} eventType={} eventId={} motivo=\"{}\" (sin JSON: {})",
                    LOG_MARKER, envelope.eventType(), envelope.eventId(), motivo, e.toString());
        }
    }

    /** Solo tests: vacía la cola sin enviar. */
    int queueSize() {
        return queue.size();
    }

    private record Pending(AuditEnvelope envelope) {
    }
}
