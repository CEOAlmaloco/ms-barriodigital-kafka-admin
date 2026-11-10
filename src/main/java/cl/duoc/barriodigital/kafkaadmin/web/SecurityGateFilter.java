package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.config.InternalTokenProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Orden EP3-03: primero {@code X-Internal-Token} (401), después rol Admin (403).
 * No lee ningún {@code X-User-*} sin token válido.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class SecurityGateFilter extends OncePerRequestFilter {

    public static final String HEADER_INTERNAL_TOKEN = "X-Internal-Token";
    public static final String UNAUTHORIZED_MESSAGE = "Token interno invalido";

    private final InternalTokenProperties tokenProperties;
    private final ObjectMapper mapper;

    public SecurityGateFilter(InternalTokenProperties tokenProperties, ObjectMapper mapper) {
        this.tokenProperties = tokenProperties;
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return uri == null || !uri.startsWith("/api/kafka/");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain
    ) throws ServletException, IOException {
        if (!tokenValid(request.getHeader(HEADER_INTERNAL_TOKEN))) {
            write(response, 401, UNAUTHORIZED_MESSAGE, request.getRequestURI());
            return;
        }
        if (!AdminCaller.isAdmin(request.getHeader(AdminCaller.HEADER_USER_ROLES))) {
            write(response, 403, AdminCaller.FORBIDDEN_MESSAGE, request.getRequestURI());
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean tokenValid(String presented) {
        String expected = tokenProperties.getInternalToken();
        return expected != null && !expected.isBlank() && expected.equals(presented);
    }

    private void write(HttpServletResponse response, int status, String message, String path) throws IOException {
        response.setStatus(status);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        mapper.writeValue(response.getOutputStream(), ErrorBody.of(status, message, path));
    }
}
