package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import java.util.List;

/** Resultado de reiniciar los offsets de un grupo: posiciones ya vigentes y lag recalculado. */
public record OffsetResetResponse(String groupId, String topic, String target,
                                  List<PartitionOffsetResponse> offsets) {
}
