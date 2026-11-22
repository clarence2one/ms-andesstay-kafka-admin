package com.andesstay.ms_andesstay_kafka_admin.web.dto;

/** Miembro conectado a un consumer group, segun describeConsumerGroups. */
public record ConsumerGroupMemberResponse(String memberId, String clientId, String host) {
}
