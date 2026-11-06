package cl.duoc.barriodigital.kafkaadmin.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Consumer groups del sistema declarados en {@code barriodigital.kafka.groups}.
 * Se tratan como existentes aunque el broker los haya borrado (EP3-03, 6.1).
 */
@Component
@ConfigurationProperties(prefix = "barriodigital.kafka")
public class KafkaGroupsProperties {

    private Map<String, GroupSpec> groups = new LinkedHashMap<>();

    /** Grupos declarados en el yml, por groupId. */
    public Map<String, GroupSpec> getGroups() {
        return groups;
    }

    /** Spring asigna el mapa leído del yml. */
    public void setGroups(Map<String, GroupSpec> groups) {
        this.groups = groups;
    }

    /** Un consumer group del sistema y los tópicos que consume. */
    public static class GroupSpec {

        private List<String> topics = List.of();

        /** Tópicos que el grupo consume, declarados en EP3-01. */
        public List<String> getTopics() {
            return topics;
        }

        /** Spring asigna la lista de tópicos leída del yml. */
        public void setTopics(List<String> topics) {
            this.topics = topics;
        }
    }
}
