package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import java.util.List;

/**
 * Estado de un consumer group. En GET /consumer-groups los offsets y miembros se devuelven
 * vacios (listConsumerGroups solo da el group.id y su estado); en GET /consumer-groups/{group}
 * se completan con listConsumerGroupOffsets, endOffsets y describeConsumerGroups.
 */
public record ConsumerGroupResponse(String groupId, String state, List<PartitionOffsetResponse> offsets,
                                    List<ConsumerGroupMemberResponse> members) {
}
