package cl.duoc.barriodigital.kafkaadmin.adapter.in.web.dto;

import java.util.List;

/** Detalle de un consumer group con miembros y lag por tópico y partición. */
public record GroupDetailView(
        String groupId,
        String state,
        int members,
        boolean system,
        List<GroupMemberView> memberDetails,
        List<GroupTopicView> topics) {

    /** Miembro conectado al grupo (AdminClient). */
    public record GroupMemberView(String memberId, String clientId, String host) {
    }

    /** Lag de un tópico del grupo. */
    public record GroupTopicView(String topic, List<PartitionLagView> partitions) {
    }

    /**
     * Lag de una partición: distancia de offsets entre el final y el offset
     * confirmado (COMMITTED), o entre el final y el primer offset disponible
     * para grupos del sistema sin offset (EARLIEST). En otros grupos sin
     * offset, lag y lagBasis son null.
     */
    public record PartitionLagView(
            int partition,
            Long committedOffset,
            long endOffset,
            Long lag,
            String lagBasis) {
    }
}
