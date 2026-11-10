package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.messaging.ActorContext;
import cl.duoc.barriodigital.kafkaadmin.messaging.AuditActor;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Tras el token y el rol Admin, guarda el actor de auditoría.
 * No se ejecuta si {@link SecurityGateFilter} ya cortó la cadena.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class ActorContextFilter extends OncePerRequestFilter {

    public static final String HEADER_USER_ID = "X-User-Id";
    public static final String HEADER_USER_NAME = "X-User-Name";
    public static final String HEADER_SOURCE_IP = "X-Source-Ip";

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
        try {
            ActorContext.set(from(request));
            chain.doFilter(request, response);
        } finally {
            ActorContext.clear();
        }
    }

    static AuditActor from(HttpServletRequest request) {
        String id = blankToNull(request.getHeader(HEADER_USER_ID));
        if (id == null) {
            id = "unknown";
        }
        String name = decodeName(request.getHeader(HEADER_USER_NAME));
        if (name == null || name.isBlank()) {
            name = id;
        }
        String role = resolveRole(request.getHeader(AdminCaller.HEADER_USER_ROLES));
        String sourceIp = blankToNull(request.getHeader(HEADER_SOURCE_IP));
        return new AuditActor(id, name, role, sourceIp);
    }

    private static String resolveRole(String rolesHeader) {
        if (rolesHeader == null || rolesHeader.isBlank()) {
            return "Admin";
        }
        for (String part : rolesHeader.split(",")) {
            String role = part.trim();
            if (role.equalsIgnoreCase("Admin") || role.equalsIgnoreCase("ROLE_Admin")) {
                return "Admin";
            }
            if (role.regionMatches(true, 0, "ROLE_", 0, 5)) {
                return role.substring(5);
            }
            if (!role.isEmpty()) {
                return role;
            }
        }
        return "Admin";
    }

    private static String decodeName(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return raw;
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
