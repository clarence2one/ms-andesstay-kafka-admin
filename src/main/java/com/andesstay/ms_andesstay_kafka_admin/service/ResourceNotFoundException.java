package com.andesstay.ms_andesstay_kafka_admin.service;

/** El topico o el consumer group indicado no existe en el broker (HTTP 404). */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
