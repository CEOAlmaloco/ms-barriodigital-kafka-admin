package cl.duoc.barriodigital.kafkaadmin.adapter.out.messaging;

/**
 * Actor de la petición actual (headers X-User-* / X-Source-Ip).
 * Solo se llena después del token interno válido.
 */
public final class ActorContext {

    private static final ThreadLocal<AuditActor> HOLDER = new ThreadLocal<>();

    private ActorContext() {
    }

    public static void set(AuditActor actor) {
        HOLDER.set(actor);
    }

    public static AuditActor get() {
        return HOLDER.get();
    }

    public static void clear() {
        HOLDER.remove();
    }
}
