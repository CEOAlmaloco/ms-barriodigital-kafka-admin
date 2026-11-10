package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.DltService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Listado y replay selectivo de las DLT del sistema (EP3-21 / EP3-03, 7). */
@RestController
@RequestMapping("/api/kafka/dlt")
public class DltController {

    private final DltService dltService;

    public DltController(DltService dltService) {
        this.dltService = dltService;
    }

    /** GET /api/kafka/dlt/{name}/messages — lectura sin group.id, paginada por partición. */
    @GetMapping("/{name}/messages")
    public DltMessagesView list(
            @PathVariable String name,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) Long fromOffset,
            @RequestParam(required = false) Integer limit
    ) {
        return dltService.listMessages(name, partition, fromOffset, limit);
    }

    /** POST /api/kafka/dlt/{name}/replay — reenvío selectivo al tópico original. */
    @PostMapping("/{name}/replay")
    public DltReplayResponse replay(
            @PathVariable String name,
            @Valid @RequestBody DltReplayRequest request
    ) {
        return dltService.replay(name, request);
    }
}
