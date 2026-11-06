package cl.duoc.barriodigital.kafkaadmin.web;

/** Pedido de aumento de particiones; solo crece, nunca disminuye. */
public record UpdatePartitionsRequest(Integer partitions) {
}
