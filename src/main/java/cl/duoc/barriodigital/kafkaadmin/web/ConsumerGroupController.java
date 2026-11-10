package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.ConsumerGroupService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Consumer groups: listado, detalle con lag y reset de offsets (EP3-03, 6). */
@RestController
@RequestMapping("/api/kafka/consumer-groups")
public class ConsumerGroupController {

    private final ConsumerGroupService consumerGroupService;

    public ConsumerGroupController(ConsumerGroupService consumerGroupService) {
        this.consumerGroupService = consumerGroupService;
    }

    /** GET /api/kafka/consumer-groups — grupos del broker más los del sistema. */
    @GetMapping
    public List<GroupSummaryView> list() {
        return consumerGroupService.listGroups();
    }

    /** GET /api/kafka/consumer-groups/{groupId} — lag por tópico y partición. */
    @GetMapping("/{groupId}")
    public GroupDetailView detail(@PathVariable String groupId) {
        return consumerGroupService.describeGroup(groupId);
    }

    /** POST /api/kafka/consumer-groups/{groupId}/offsets/reset — dry run o real. */
    @PostMapping("/{groupId}/offsets/reset")
    public ResetOffsetsResponse reset(
            @PathVariable String groupId,
            @Valid @RequestBody ResetOffsetsRequest request
    ) {
        return consumerGroupService.resetOffsets(groupId, request);
    }
}
