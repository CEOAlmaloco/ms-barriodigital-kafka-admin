package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/** Errores del API con el cuerpo del contrato: timestamp, status, error, message y path. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<Map<String, Object>> kafkaUnavailable(KafkaUnavailableException ex, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorBody.of(503, "Kafka no esta disponible", request.getRequestURI()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> status(ResponseStatusException ex, HttpServletRequest request) {
        int code = ex.getStatusCode().value();
        String message = ex.getReason() == null ? ex.getStatusCode().toString() : ex.getReason();
        return ResponseEntity.status(code).body(ErrorBody.of(code, message, request.getRequestURI()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(FieldError::getDefaultMessage)
                .orElse("El cuerpo no cumple el formato");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorBody.of(400, message, request.getRequestURI()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> unreadable(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorBody.of(400, "El cuerpo no cumple el formato", request.getRequestURI()));
    }
}
