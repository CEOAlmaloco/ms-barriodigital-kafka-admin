package cl.duoc.barriodigital.kafkaadmin.adapter.in.web.dto;

/** Un consumer group en el listado. */
public record GroupSummaryView(String groupId, String state, int members, boolean system) {
}
