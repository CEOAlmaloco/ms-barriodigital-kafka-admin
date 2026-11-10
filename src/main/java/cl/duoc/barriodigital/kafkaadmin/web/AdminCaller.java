package cl.duoc.barriodigital.kafkaadmin.web;

/**
 * Comprueba el rol Admin que reenvía el BFF en {@code X-User-Roles}.
 * Misma regla que mq-admin (EP2-04 / EP3-03, 1.3).
 */
public final class AdminCaller {

    public static final String HEADER_USER_ROLES = "X-User-Roles";
    public static final String FORBIDDEN_MESSAGE = "Solo Admin puede administrar Kafka";

    private AdminCaller() {
    }

    public static boolean isAdmin(String rolesHeader) {
        if (rolesHeader == null || rolesHeader.isBlank()) {
            return false;
        }
        for (String part : rolesHeader.split(",")) {
            String role = part.trim();
            if (role.equalsIgnoreCase("Admin") || role.equalsIgnoreCase("ROLE_Admin")) {
                return true;
            }
        }
        return false;
    }
}
