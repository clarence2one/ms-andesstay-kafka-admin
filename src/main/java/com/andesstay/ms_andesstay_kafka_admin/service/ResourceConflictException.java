package com.andesstay.ms_andesstay_kafka_admin.service;

/**
 * La operacion choca con el estado del broker (HTTP 409): el topico ya existe, es parte de la
 * topologia del sistema, o el grupo tiene miembros activos y no permite alterar offsets.
 */
public class ResourceConflictException extends RuntimeException {

    public ResourceConflictException(String message) {
        super(message);
    }
}
