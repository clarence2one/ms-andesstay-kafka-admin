package com.andesstay.ms_andesstay_kafka_admin.service;

/** No hay conexion con Kafka o el AdminClient agoto el timeout (HTTP 503). */
public class BrokerUnavailableException extends RuntimeException {

    public BrokerUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
