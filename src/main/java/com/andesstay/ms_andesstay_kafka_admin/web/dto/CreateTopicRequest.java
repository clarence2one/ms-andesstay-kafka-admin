package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/**
 * Cuerpo de POST /api/kafka/topics. Las configuraciones son opcionales y se envian
 * tal cual a NewTopic.configs (por ejemplo {"retention.ms": "3600000"}).
 */
public record CreateTopicRequest(
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = ResourceNames.TOPIC_NAME_REGEX, message = ResourceNames.TOPIC_NAME_MESSAGE)
        String name,

        @Min(value = 1, message = "debe ser al menos 1")
        @Max(value = 100, message = "no puede superar 100")
        int partitions,

        @Min(value = 1, message = "debe ser al menos 1")
        @Max(value = 10, message = "no puede superar 10")
        short replicationFactor,

        Map<@NotBlank(message = "la clave de configuracion no puede estar vacia") String,
                @NotBlank(message = "el valor de configuracion no puede estar vacio") String> configs
) {
}
