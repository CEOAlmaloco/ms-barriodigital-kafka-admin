package cl.duoc.barriodigital.kafkaadmin.adapter.in.web.dto;

import java.util.List;

/** Página de una partición de la DLT. {@code nextOffset} es null al llegar al final. */
public record DltPartitionView(
        int partition,
        Long nextOffset,
        List<DltMessageView> messages
) {
}
