package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.TopicService;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Tópicos del Admin: consulta, alta, configuraciones, particiones y baja (EP3-03, 2 a 5). */
@RestController
@RequestMapping("/api/kafka/topics")
public class TopicController {

    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    /** GET /api/kafka/topics — lista con system, sin tópicos internos. */
    @GetMapping
    public List<TopicSummaryView> list() {
        return topicService.listTopics();
    }

    /** GET /api/kafka/topics/{name} — particiones y configuraciones. */
    @GetMapping("/{name}")
    public TopicDetailView detail(@PathVariable String name) {
        return topicService.describeTopic(name);
    }

    /** POST /api/kafka/topics — alta de un tópico del Admin. */
    @PostMapping
    public ResponseEntity<TopicDetailView> create(@RequestBody CreateTopicRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(topicService.createTopic(request));
    }

    /** PATCH /api/kafka/topics/{name}/configs — cambio parcial de configuraciones. */
    @PatchMapping("/{name}/configs")
    public TopicDetailView updateConfigs(@PathVariable String name,
            @RequestBody UpdateTopicConfigsRequest request) {
        return topicService.updateConfigs(name, request);
    }

    /** PATCH /api/kafka/topics/{name}/partitions — solo aumenta particiones. */
    @PatchMapping("/{name}/partitions")
    public TopicDetailView updatePartitions(@PathVariable String name,
            @RequestBody UpdatePartitionsRequest request) {
        return topicService.updatePartitions(name, request);
    }

    /** DELETE /api/kafka/topics/{name} — baja de un tópico del Admin. */
    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable String name) {
        topicService.deleteTopic(name);
        return ResponseEntity.noContent().build();
    }
}
