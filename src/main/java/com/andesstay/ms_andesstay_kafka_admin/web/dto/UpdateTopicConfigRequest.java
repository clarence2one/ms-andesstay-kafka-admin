package com.andesstay.ms_andesstay_kafka_admin.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.Map;

/**
 * Cuerpo de PATCH /api/kafka/topics/{name}/configs. Solo se cambian las claves enviadas:
 * el servicio usa incrementalAlterConfigs con AlterConfigOp.OpType.SET, no reemplaza toda
 * la configuracion del topico.
 */
public record UpdateTopicConfigRequest(
        @NotEmpty(message = "es obligatorio: envie al menos una configuracion")
        Map<@NotBlank(message = "la clave de configuracion no puede estar vacia") String,
                @NotBlank(message = "el valor de configuracion no puede estar vacio") String> configs
) {
}
