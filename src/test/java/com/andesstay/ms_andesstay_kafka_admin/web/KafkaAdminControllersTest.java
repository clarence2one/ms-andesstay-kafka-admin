package com.andesstay.ms_andesstay_kafka_admin.web;

import com.andesstay.ms_andesstay_kafka_admin.config.SecurityConfig;
import com.andesstay.ms_andesstay_kafka_admin.config.SecurityProperties;
import com.andesstay.ms_andesstay_kafka_admin.service.BrokerUnavailableException;
import com.andesstay.ms_andesstay_kafka_admin.service.KafkaAdminService;
import com.andesstay.ms_andesstay_kafka_admin.service.ResourceConflictException;
import com.andesstay.ms_andesstay_kafka_admin.service.ResourceNotFoundException;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ConsumerGroupMemberResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ConsumerGroupResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.CreateTopicRequest;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.OffsetResetResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.OffsetTarget;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.PartitionOffsetResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ResetOffsetRequest;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.TopicResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Seguridad por rol, validacion de entrada y codigos HTTP de la API. El servicio se mocks
 * para no depender de un broker Kafka ni de Azure AD.
 */
@WebMvcTest({TopicController.class, ConsumerGroupController.class})
@Import({SecurityConfig.class, KafkaAdminControllersTest.TestSecurityProperties.class})
class KafkaAdminControllersTest {

    /** En un slice @WebMvcTest no corre @ConfigurationPropertiesScan: se registran aqui. */
    @TestConfiguration
    @EnableConfigurationProperties(SecurityProperties.class)
    static class TestSecurityProperties {
    }

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private KafkaAdminService service;

    /** Evita que SecurityConfig construya un decoder que intentaria consultar el issuer. */
    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void sinTokenResponde401() throws Exception {
        mvc.perform(get("/api/kafka/topics"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.message").value("No autenticado: token ausente, invalido o vencido"));
    }

    @Test
    void unRolDistintoDeAdminResponde403() throws Exception {
        mvc.perform(get("/api/kafka/topics").with(role("Operador")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message").value("El rol del usuario no tiene permiso para este recurso"));
    }

    @Test
    void cicloCompletoDeTopicos() throws Exception {
        TopicResponse created = new TopicResponse("it.demo", 3, 1, false, Map.of());
        when(service.createTopic(any(CreateTopicRequest.class))).thenReturn(created);
        when(service.listTopics()).thenReturn(List.of(created));
        when(service.describeTopic("it.demo")).thenReturn(
                new TopicResponse("it.demo", 3, 1, false, Map.of("retention.ms", "3600000")));
        when(service.updateTopicConfig(eq("it.demo"), any())).thenReturn(
                new TopicResponse("it.demo", 3, 1, false, Map.of("retention.ms", "3600000")));

        mvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"it.demo\",\"partitions\":3,\"replicationFactor\":1}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/api/kafka/topics/it.demo"))
                .andExpect(jsonPath("$.partitions").value(3));

        mvc.perform(get("/api/kafka/topics").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("it.demo"))
                .andExpect(jsonPath("$[0].replicationFactor").value(1));

        mvc.perform(get("/api/kafka/topics/it.demo").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configs['retention.ms']").value("3600000"));

        mvc.perform(patch("/api/kafka/topics/it.demo/configs").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configs\":{\"retention.ms\":\"3600000\"}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configs['retention.ms']").value("3600000"));

        mvc.perform(delete("/api/kafka/topics/it.demo").with(admin()))
                .andExpect(status().isNoContent());
        verify(service).deleteTopic("it.demo");
    }

    @Test
    void cicloCompletoDeConsumerGroupsConLag() throws Exception {
        ConsumerGroupResponse summary = new ConsumerGroupResponse("g.demo", "STABLE", List.of(), List.of());
        ConsumerGroupResponse detail = new ConsumerGroupResponse("g.demo", "STABLE",
                List.of(new PartitionOffsetResponse(0, 10, 15, 5)),
                List.of(new ConsumerGroupMemberResponse("member-1", "client-1", "/127.0.0.1")));
        when(service.listConsumerGroups()).thenReturn(List.of(summary));
        when(service.describeConsumerGroup("g.demo")).thenReturn(detail);
        when(service.resetConsumerGroupOffset(eq("g.demo"), any(ResetOffsetRequest.class)))
                .thenReturn(new OffsetResetResponse("g.demo", "it.demo", "EARLIEST",
                        List.of(new PartitionOffsetResponse(0, 0, 15, 15))));

        mvc.perform(get("/api/kafka/consumer-groups").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].groupId").value("g.demo"));

        mvc.perform(get("/api/kafka/consumer-groups/g.demo").with(admin()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.offsets[0].currentOffset").value(10))
                .andExpect(jsonPath("$.offsets[0].logEndOffset").value(15))
                .andExpect(jsonPath("$.offsets[0].lag").value(5))
                .andExpect(jsonPath("$.members[0].memberId").value("member-1"));

        mvc.perform(post("/api/kafka/consumer-groups/g.demo/offsets/reset").with(admin())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"it.demo\",\"target\":\"EARLIEST\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.target").value("EARLIEST"))
                .andExpect(jsonPath("$.offsets[0].lag").value(15));

        ArgumentCaptor<ResetOffsetRequest> request = ArgumentCaptor.forClass(ResetOffsetRequest.class);
        verify(service).resetConsumerGroupOffset(eq("g.demo"), request.capture());
        assertThat(request.getValue().topic()).isEqualTo("it.demo");
        assertThat(request.getValue().target()).isEqualTo(OffsetTarget.EARLIEST);
    }

    @Test
    void datosInvalidosResponden400() throws Exception {
        mvc.perform(post("/api/kafka/topics").with(admin()).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"  \",\"partitions\":0,\"replicationFactor\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.*", hasItem(containsString("debe ser al menos 1"))))
                .andExpect(jsonPath("$.errors.*", hasItem(containsString("es obligatorio"))));

        // En Spring Boot 4 una violacion sobre el record entero se informa con el nombre
        // del parametro ("request"), no con el campo; se valida el mensaje, no la clave.
        mvc.perform(patch("/api/kafka/topics/it.demo/configs").with(admin())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"configs\":{}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.*", hasItem(containsString("configuracion"))));

        mvc.perform(post("/api/kafka/consumer-groups/g.demo/offsets/reset").with(admin())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"topic\":\"it.demo\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.*", hasItem(containsString("EARLIEST o LATEST"))));

        mvc.perform(get("/api/kafka/topics/{name}", "nombre con espacios").with(admin()))
                .andExpect(status().isBadRequest());
    }

    @Test
    void erroresDelDominioSeTraducenAHttp() throws Exception {
        when(service.describeTopic("it.no.existe"))
                .thenThrow(new ResourceNotFoundException("El topico 'it.no.existe' no existe"));
        mvc.perform(get("/api/kafka/topics/it.no.existe").with(admin()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("El topico 'it.no.existe' no existe"));

        doThrow(new ResourceConflictException(
                "El topico 'reservations.events' es parte de la topologia del sistema"))
                .when(service).deleteTopic("reservations.events");
        mvc.perform(delete("/api/kafka/topics/reservations.events").with(admin()))
                .andExpect(status().isConflict());

        when(service.listTopics()).thenThrow(new BrokerUnavailableException("Sin conexion con Kafka",
                new IOException("Connection refused")));
        mvc.perform(get("/api/kafka/topics").with(admin()))
                .andExpect(status().isServiceUnavailable());
    }

    private static RequestPostProcessor admin() {
        return role("Admin");
    }

    private static RequestPostProcessor role(String role) {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_" + role));
    }
}
