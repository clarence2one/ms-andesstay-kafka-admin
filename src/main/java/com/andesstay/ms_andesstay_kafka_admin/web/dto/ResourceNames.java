package com.andesstay.ms_andesstay_kafka_admin.web.dto;

/** Reglas de nombres compartidas por los DTOs y los parametros de ruta. */
public final class ResourceNames {

    /**
     * Un topico Kafka tiene entre 1 y 249 caracteres, integrados por letras, digitos,
     * '.', '_' o '-'. El primer caracter es alfanumerico (Kafka no admite '.' ni '_' iniciales).
     */
    public static final String TOPIC_NAME_REGEX = "^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$";
    public static final String TOPIC_NAME_MESSAGE =
            "solo puede tener letras, numeros, '.', '_' o '-' (1 a 249 caracteres) y empezar por letra o numero";

    /** Los group.id aceptan los mismos caracteres de los topicos, con longitud mas corta. */
    public static final String GROUP_ID_REGEX = "^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$";
    public static final String GROUP_ID_MESSAGE =
            "solo puede tener letras, numeros, '.', '_' o '-' (1 a 200 caracteres) y empezar por letra o numero";

    private ResourceNames() {
    }
}
