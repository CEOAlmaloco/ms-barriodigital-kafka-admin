package cl.duoc.barriodigital.kafkaadmin.web;

import java.util.List;

/** Resultado del reset de offsets, con la misma forma con y sin dryRun. */
public record ResetOffsetsResponse(
        String groupId,
        String topic,
        String strategy,
        boolean dryRun,
        List<PartitionResetView> partitions) {

    /** Offset actual, resultante y lag resultante de una partición. */
    public record PartitionResetView(int partition, Long currentOffset, long targetOffset, long lagAfter) {
    }
}
