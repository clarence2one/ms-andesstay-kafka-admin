package com.andesstay.ms_andesstay_kafka_admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Topicos que la API no permite eliminar ni reconfigurar (andesstay.kafka-admin en application.yml)
 * y timeouts del AdminClient, todos en milisegundos.
 */
@ConfigurationProperties(prefix = "andesstay.kafka-admin")
public record KafkaAdminProperties(List<String> protectedTopics, int requestTimeoutMs, int defaultApiTimeoutMs) {

    public KafkaAdminProperties {
        protectedTopics = protectedTopics == null ? List.of() : List.copyOf(protectedTopics);
        requestTimeoutMs = requestTimeoutMs > 0 ? requestTimeoutMs : 10_000;
        defaultApiTimeoutMs = defaultApiTimeoutMs > 0 ? defaultApiTimeoutMs : 10_000;
    }

    public boolean isProtectedTopic(String name) {
        return protectedTopics.contains(name);
    }
}
