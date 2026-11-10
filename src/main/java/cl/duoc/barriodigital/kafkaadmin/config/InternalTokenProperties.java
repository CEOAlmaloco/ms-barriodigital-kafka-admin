package cl.duoc.barriodigital.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Token compartido BFF ↔ kafka-admin ({@code X-Internal-Token}). */
@Component
@ConfigurationProperties(prefix = "barriodigital.security")
public class InternalTokenProperties {

    /** Valor esperado del header. Viene de {@code INTERNAL_TOKEN}. */
    private String internalToken = "";

    public String getInternalToken() {
        return internalToken;
    }

    public void setInternalToken(String internalToken) {
        this.internalToken = internalToken == null ? "" : internalToken;
    }
}
