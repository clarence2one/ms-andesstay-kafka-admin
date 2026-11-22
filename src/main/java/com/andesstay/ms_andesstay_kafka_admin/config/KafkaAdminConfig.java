package com.andesstay.ms_andesstay_kafka_admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/** Unico AdminClient de la aplicacion: todas las operaciones de administracion pasan por el. */
@Configuration
public class KafkaAdminConfig {

    private final KafkaAdminProperties properties;
    private final String bootstrapServers;

    public KafkaAdminConfig(KafkaAdminProperties properties,
                            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.properties = properties;
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * AdminClient.create no abre conexion: el broker se contacta recien en cada llamada.
     * Spring cierra el cliente al apagar el contexto mediante su metodo close().
     */
    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "andesstay-kafka-admin");
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.requestTimeoutMs());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, properties.defaultApiTimeoutMs());
        return AdminClient.create(config);
    }
}
