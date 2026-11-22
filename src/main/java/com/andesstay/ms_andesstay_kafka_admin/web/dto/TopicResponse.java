package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import java.util.Map;

/**
 * Estado de un topico. En GET /topics las configuraciones se devuelven vacias (no se
 * consultan para no hacer un describeConfigs por topico); en GET /topics/{name} y en
 * PATCH se devuelven las configuraciones efectivas del broker.
 */
public record TopicResponse(String name, int partitions, int replicationFactor, boolean internal,
                            Map<String, String> configs) {
}
