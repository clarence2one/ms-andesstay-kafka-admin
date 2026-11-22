package com.andesstay.ms_andesstay_kafka_admin.web;

import com.andesstay.ms_andesstay_kafka_admin.service.KafkaAdminService;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ConsumerGroupResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.OffsetResetResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ResourceNames;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ResetOffsetRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Consumer groups de Kafka.
 * <ul>
 *   <li>GET  /api/kafka/consumer-groups                          lista grupos y estados</li>
 *   <li>GET  /api/kafka/consumer-groups/{group}                  offsets por particion y miembros</li>
 *   <li>POST /api/kafka/consumer-groups/{group}/offsets/reset    reinicia offsets a EARLIEST/LATEST</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/kafka/consumer-groups")
@PreAuthorize("hasRole('Admin')")
public class ConsumerGroupController {

    private final KafkaAdminService kafkaAdminService;

    public ConsumerGroupController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping
    public List<ConsumerGroupResponse> list() {
        return kafkaAdminService.listConsumerGroups();
    }

    @GetMapping("/{group}")
    public ConsumerGroupResponse get(@PathVariable @Pattern(regexp = ResourceNames.GROUP_ID_REGEX,
            message = ResourceNames.GROUP_ID_MESSAGE) String group) {
        return kafkaAdminService.describeConsumerGroup(group);
    }

    @PostMapping("/{group}/offsets/reset")
    public OffsetResetResponse reset(@PathVariable @Pattern(regexp = ResourceNames.GROUP_ID_REGEX,
            message = ResourceNames.GROUP_ID_MESSAGE) String group,
                                     @Valid @RequestBody ResetOffsetRequest request) {
        return kafkaAdminService.resetConsumerGroupOffset(group, request);
    }
}
