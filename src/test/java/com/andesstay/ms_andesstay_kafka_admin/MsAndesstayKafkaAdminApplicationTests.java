package com.andesstay.ms_andesstay_kafka_admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Verifica que el contexto arme sin necesidad de Kafka ni de Azure AD. */
@SpringBootTest
class MsAndesstayKafkaAdminApplicationTests {

    /**
     * El JwtDecoder real consulta los metadatos del issuer al construirse. Se reemplaza por un
     * mock: AdminClient.create tampoco conecta, asi que este test no requiere broker levantado.
     */
    @MockitoBean
    private JwtDecoder jwtDecoder;

    /** El AdminClient real abre hilos de metadata; el test solo verifica el contexto. */
    @MockitoBean
    private AdminClient adminClient;

    @Test
    void contextLoads() {
    }
}
