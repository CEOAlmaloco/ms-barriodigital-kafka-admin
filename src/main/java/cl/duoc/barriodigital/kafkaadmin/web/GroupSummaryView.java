package cl.duoc.barriodigital.kafkaadmin.web;

/** Un consumer group en el listado. */
public record GroupSummaryView(String groupId, String state, int members, boolean system) {
}
