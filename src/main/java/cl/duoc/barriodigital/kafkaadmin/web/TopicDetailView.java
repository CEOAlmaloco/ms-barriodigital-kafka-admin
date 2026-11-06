package cl.duoc.barriodigital.kafkaadmin.web;

import java.util.List;

/** Detalle de un tópico: particiones con líder/réplicas/ISR y configuraciones. */
public record TopicDetailView(String name, boolean system, List<PartitionView> partitions,
        List<ConfigView> configs) {

    /** Una partición del tópico. */
    public record PartitionView(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {
    }

    /** Una configuración con su valor, origen (TOPIC/DEFAULT/BROKER) y si es editable. */
    public record ConfigView(String name, String value, String source, boolean editable) {
    }
}
