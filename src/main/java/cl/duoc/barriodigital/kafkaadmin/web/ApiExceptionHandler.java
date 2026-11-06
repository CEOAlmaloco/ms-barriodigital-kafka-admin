package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/** Errores del API con el cuerpo del contrato: status, error, message y path. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<Map<String, Object>> kafkaUnavailable(KafkaUnavailableException ex, HttpServletRequest request) {
        return body(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no esta disponible", request);
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> status(ResponseStatusException ex, HttpServletRequest request) {
        String message = ex.getReason() == null ? ex.getStatusCode().toString() : ex.getReason();
        return body(HttpStatus.valueOf(ex.getStatusCode().value()), message, request);
    }

    private ResponseEntity<Map<String, Object>> body(HttpStatus status, String message, HttpServletRequest request) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", status.value());
        payload.put("error", status.getReasonPhrase());
        payload.put("message", message);
        payload.put("path", request.getRequestURI());
        return ResponseEntity.status(status).body(payload);
    }
}
