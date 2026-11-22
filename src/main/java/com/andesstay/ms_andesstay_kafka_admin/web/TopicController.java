package com.andesstay.ms_andesstay_kafka_admin.web;

import com.andesstay.ms_andesstay_kafka_admin.service.KafkaAdminService;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.CreateTopicRequest;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ResourceNames;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.TopicResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.UpdateTopicConfigRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

/**
 * Topicos de Kafka.
 * <ul>
 *   <li>POST   /api/kafka/topics                    crea un topico (201, 409 si ya existe)</li>
 *   <li>GET    /api/kafka/topics                    lista con particiones y replicas (200)</li>
 *   <li>GET    /api/kafka/topics/{name}             describe con configuraciones (404 si no existe)</li>
 *   <li>PATCH  /api/kafka/topics/{name}/configs     modifica configuraciones de forma incremental (200)</li>
 *   <li>DELETE /api/kafka/topics/{name}             elimina (204; 404 si no existe; 409 si es del sistema)</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/kafka/topics")
@PreAuthorize("hasRole('Admin')")
public class TopicController {

    private final KafkaAdminService kafkaAdminService;

    public TopicController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @PostMapping
    public ResponseEntity<TopicResponse> create(@Valid @RequestBody CreateTopicRequest request) {
        TopicResponse created = kafkaAdminService.createTopic(request);
        return ResponseEntity.created(URI.create("/api/kafka/topics/" + created.name())).body(created);
    }

    @GetMapping
    public List<TopicResponse> list() {
        return kafkaAdminService.listTopics();
    }

    @GetMapping("/{name}")
    public TopicResponse get(@PathVariable @Pattern(regexp = ResourceNames.TOPIC_NAME_REGEX,
            message = ResourceNames.TOPIC_NAME_MESSAGE) String name) {
        return kafkaAdminService.describeTopic(name);
    }

    @PatchMapping("/{name}/configs")
    public TopicResponse updateConfig(@PathVariable @Pattern(regexp = ResourceNames.TOPIC_NAME_REGEX,
            message = ResourceNames.TOPIC_NAME_MESSAGE) String name,
                                      @Valid @RequestBody UpdateTopicConfigRequest request) {
        return kafkaAdminService.updateTopicConfig(name, request);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable @Pattern(regexp = ResourceNames.TOPIC_NAME_REGEX,
            message = ResourceNames.TOPIC_NAME_MESSAGE) String name) {
        kafkaAdminService.deleteTopic(name);
        return ResponseEntity.noContent().build();
    }
}
