package cl.duoc.barriodigital.kafkaadmin.web;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;

/**
 * JSON de error uniforme (EP3-03, 1.5), igual que mq-admin:
 * timestamp, status, error, message y path.
 */
public final class ErrorBody {

    private ErrorBody() {
    }

    public static Map<String, Object> of(int status, String message, String path) {
        HttpStatus resolved = HttpStatus.resolve(status);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.now(ZoneOffset.UTC)));
        body.put("status", status);
        body.put("error", resolved == null ? "Error" : resolved.getReasonPhrase());
        body.put("message", message);
        body.put("path", path);
        return body;
    }
}
