package com.andesstay.ms_andesstay_kafka_admin.web.dto;

/**
 * Posicion de una particion de un consumer group.
 * Lag = LogEndOffset - CurrentOffset (formula de la materia DSY1107, unidad 3.3.1).
 */
public record PartitionOffsetResponse(int partition, long currentOffset, long logEndOffset, long lag) {
}
