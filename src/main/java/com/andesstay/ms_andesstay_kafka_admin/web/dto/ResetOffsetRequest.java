package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/** Cuerpo de POST /api/kafka/consumer-groups/{group}/offsets/reset. */
public record ResetOffsetRequest(
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = ResourceNames.TOPIC_NAME_REGEX, message = ResourceNames.TOPIC_NAME_MESSAGE)
        String topic,

        @NotNull(message = "es obligatorio (EARLIEST o LATEST)")
        OffsetTarget target
) {
}
