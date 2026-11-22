package com.andesstay.ms_andesstay_kafka_admin.service;

import com.andesstay.ms_andesstay_kafka_admin.config.KafkaAdminProperties;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ConsumerGroupMemberResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ConsumerGroupResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.CreateTopicRequest;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.OffsetResetResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.OffsetTarget;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.PartitionOffsetResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.ResetOffsetRequest;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.TopicResponse;
import com.andesstay.ms_andesstay_kafka_admin.web.dto.UpdateTopicConfigRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Toda la administracion de Kafka pasa por aca: los controladores llaman a estos metodos de
 * alto nivel y reciben DTOs o excepciones del dominio ({@link ResourceNotFoundException},
 * {@link ResourceConflictException}, {@link BrokerUnavailableException}), sin conocer AdminClient,
 * futures ni codigos de error del protocolo.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;
    private final KafkaAdminProperties properties;

    public KafkaAdminService(AdminClient adminClient, KafkaAdminProperties properties) {
        this.adminClient = adminClient;
        this.properties = properties;
    }

    // ---- Topicos ----

    /** Crea el topico con NewTopic(nombre, particiones, factor de replicacion).configs(map). */
    public TopicResponse createTopic(CreateTopicRequest request) {
        if (topicExists(request.name())) {
            throw new ResourceConflictException("El topico '" + request.name() + "' ya existe");
        }
        Map<String, String> configs = configsOrEmpty(request.configs());
        // Las configuraciones son opcionales al crear; si se envian, se valida que no esten vacias.
        if (!configs.isEmpty()) {
            validateConfigs(configs);
        }

        NewTopic topic = new NewTopic(request.name(), request.partitions(), request.replicationFactor())
                .configs(configs);
        await(adminClient.createTopics(List.of(topic)).all(), "crear el topico " + request.name());
        log.info("Topico creado: {} (particiones={}, replicacion={}, configs={})",
                topic.name(), topic.numPartitions(), topic.replicationFactor(), configs);
        return describeTopic(request.name());
    }

    /** listTopics() + describeTopics(): nombre, particiones y cantidad de replicas por particion. */
    public List<TopicResponse> listTopics() {
        Set<String> names = await(adminClient.listTopics().names(), "listar topicos");
        if (names.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descriptions = await(
                adminClient.describeTopics(names).allTopicNames(), "describir topicos");
        return descriptions.values().stream()
                .map(description -> toResponse(description, Map.of()))
                .sorted(Comparator.comparing(TopicResponse::name))
                .toList();
    }

    /** describeTopics() + describeConfigs(): agrega la configuracion efectiva del topico. */
    public TopicResponse describeTopic(String name) {
        TopicDescription description = requireTopic(name);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = await(adminClient.describeConfigs(List.of(resource)).all(),
                "describir la configuracion de " + name).get(resource);
        Map<String, String> configs = new LinkedHashMap<>();
        if (config != null) {
            config.entries().stream()
                    // No exponemos secretos del broker (por ejemplo configs de SSL).
                    .filter(entry -> !entry.isSensitive() && entry.value() != null)
                    .sorted(Comparator.comparing(ConfigEntry::name))
                    .forEach(entry -> configs.put(entry.name(), entry.value()));
        }
        return toResponse(description, configs);
    }

    /**
     * Modificacion incremental: solo SET de las claves enviadas, sin reemplazar toda la
     * configuracion. En kafka-clients 4.x el enum es AlterConfigOp.OpType.SET.
     */
    public TopicResponse updateTopicConfig(String name, UpdateTopicConfigRequest request) {
        requireNotProtected(name, "modificar");
        requireTopic(name);
        Map<String, String> configs = validateConfigs(request.configs());

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        List<AlterConfigOp> changes = configs.entrySet().stream()
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()),
                        AlterConfigOp.OpType.SET))
                .toList();
        Map<ConfigResource, Collection<AlterConfigOp>> alterations = Map.of(resource, changes);
        await(adminClient.incrementalAlterConfigs(alterations).all(),
                "modificar la configuracion de " + name);
        log.info("Configuracion actualizada: {} ({})", name, configs.keySet());
        return describeTopic(name);
    }

    /** Elimina el topico. Los topicos de la topologia del sistema se rechazan con 409. */
    public void deleteTopic(String name) {
        requireNotProtected(name, "eliminar");
        requireTopic(name);
        await(adminClient.deleteTopics(List.of(name)).all(), "eliminar el topico " + name);
        log.info("Topico eliminado: {}", name);
    }

    // ---- Consumer groups ----

    /** listConsumerGroups(): identificador y estado de cada grupo. */
    public List<ConsumerGroupResponse> listConsumerGroups() {
        Collection<ConsumerGroupListing> groups = await(
                adminClient.listConsumerGroups().all(), "listar consumer groups");
        return groups.stream()
                .map(group -> new ConsumerGroupResponse(group.groupId(), stateOf(group), List.of(), List.of()))
                .sorted(Comparator.comparing(ConsumerGroupResponse::groupId))
                .toList();
    }

    /**
     * listConsumerGroupOffsets() + endOffsets() + describeConsumerGroups(): por particion
     * devuelve currentOffset, logEndOffset y lag (LogEndOffset - CurrentOffset), ademas de los
     * miembros conectados al grupo.
     */
    public ConsumerGroupResponse describeConsumerGroup(String groupId) {
        ConsumerGroupDescription description = requireConsumerGroup(groupId);
        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        Map<TopicPartition, Long> ends = endOffsets(committed.keySet());

        List<ConsumerGroupMemberResponse> members = description.members().stream()
                .map(KafkaAdminService::toMemberResponse)
                .toList();
        return new ConsumerGroupResponse(groupId, description.state().name(),
                toOffsetResponses(committed, ends), members);
    }

    /**
     * Reinicia los offsets de todas las particiones del topico a EARLIEST o LATEST.
     * beginningOffsets/endOffsets se resuelven con listOffsets (la API que expone AdminClient
     * en kafka-clients 4.x) y se aplican con alterConsumerGroupOffsets.
     */
    public OffsetResetResponse resetConsumerGroupOffset(String groupId, ResetOffsetRequest request) {
        requireConsumerGroup(groupId);
        if (request.target() == null) {
            throw new IllegalArgumentException("El objetivo de los offsets es obligatorio (EARLIEST o LATEST)");
        }

        List<TopicPartition> partitions = topicPartitions(request.topic());
        Map<TopicPartition, Long> targets = request.target() == OffsetTarget.EARLIEST
                ? beginningOffsets(partitions) : endOffsets(partitions);
        Map<TopicPartition, OffsetAndMetadata> newOffsets = new LinkedHashMap<>();
        targets.forEach((partition, offset) -> newOffsets.put(partition, new OffsetAndMetadata(offset)));

        await(adminClient.alterConsumerGroupOffsets(groupId, newOffsets).all(),
                "reiniciar los offsets del grupo " + groupId);
        log.info("Offsets reiniciados: grupo={}, topico={}, target={}, particiones={}",
                groupId, request.topic(), request.target(), partitions.size());

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        Map<TopicPartition, Long> ends = endOffsets(committed.keySet());
        return new OffsetResetResponse(groupId, request.topic(), request.target().name(),
                toOffsetResponses(committed, ends));
    }

    // ---- Internos ----

    private boolean topicExists(String name) {
        return await(adminClient.listTopics().names(), "listar topicos").contains(name);
    }

    private TopicDescription requireTopic(String name) {
        Map<String, TopicDescription> descriptions = await(
                adminClient.describeTopics(List.of(name)).allTopicNames(), "describir el topico " + name);
        TopicDescription description = descriptions.get(name);
        if (description == null) {
            throw new ResourceNotFoundException("El topico '" + name + "' no existe");
        }
        return description;
    }

    private List<TopicPartition> topicPartitions(String topic) {
        return requireTopic(topic).partitions().stream()
                .map(partition -> new TopicPartition(topic, partition.partition()))
                .toList();
    }

    private ConsumerGroupDescription requireConsumerGroup(String groupId) {
        Map<String, ConsumerGroupDescription> groups = await(
                adminClient.describeConsumerGroups(List.of(groupId)).all(), "describir el grupo " + groupId);
        ConsumerGroupDescription description = groups.get(groupId);
        if (description == null) {
            throw new ResourceNotFoundException("El consumer group '" + groupId + "' no existe");
        }
        return description;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        return await(adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "listar offsets del grupo " + groupId);
    }

    /** Equivalente a beginningOffsets: primer offset disponible de cada particion. */
    private Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return offsets(partitions, OffsetSpec.earliest());
    }

    /** Equivalente a endOffsets: log end offset (siguiente offset a escribir) de cada particion. */
    private Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return offsets(partitions, OffsetSpec.latest());
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, OffsetSpec spec) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> requested = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) {
            requested.put(partition, spec);
        }
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        await(adminClient.listOffsets(requested).all(), "consultar offsets").forEach(
                (partition, info) -> result.put(partition, info.offset()));
        return result;
    }

    /** Lag segun la materia 3.3.1: LogEndOffset - CurrentOffset. */
    private static List<PartitionOffsetResponse> toOffsetResponses(
            Map<TopicPartition, OffsetAndMetadata> committed, Map<TopicPartition, Long> ends) {
        List<PartitionOffsetResponse> offsets = new ArrayList<>();
        committed.forEach((partition, metadata) -> {
            long currentOffset = metadata.offset();
            long logEndOffset = ends.getOrDefault(partition, currentOffset);
            offsets.add(new PartitionOffsetResponse(partition.partition(), currentOffset, logEndOffset,
                    logEndOffset - currentOffset));
        });
        offsets.sort(Comparator.comparingInt(PartitionOffsetResponse::partition));
        return offsets;
    }

    private static TopicResponse toResponse(TopicDescription description, Map<String, String> configs) {
        List<TopicPartitionInfo> partitions = description.partitions();
        // El factor de replicacion es la cantidad de replicas de cualquier particion.
        int replicationFactor = partitions.isEmpty() ? 0 : partitions.get(0).replicas().size();
        return new TopicResponse(description.name(), partitions.size(), replicationFactor,
                description.isInternal(), configs);
    }

    private static String stateOf(ConsumerGroupListing group) {
        return group.state().map(state -> state.name()).orElse("UNKNOWN");
    }

    private static ConsumerGroupMemberResponse toMemberResponse(MemberDescription member) {
        return new ConsumerGroupMemberResponse(member.consumerId(), member.clientId(), member.host());
    }

    private void requireNotProtected(String name, String action) {
        if (properties.isProtectedTopic(name)) {
            throw new ResourceConflictException("El topico '" + name
                    + "' es parte de la topologia del sistema y no se puede " + action);
        }
    }

    private static Map<String, String> configsOrEmpty(Map<String, String> configs) {
        return configs == null ? Map.of() : configs;
    }

    private static Map<String, String> validateConfigs(Map<String, String> configs) {
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Debe indicar al menos una configuracion");
        }
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()
                    || entry.getValue() == null || entry.getValue().isBlank()) {
                throw new IllegalArgumentException("Las claves y valores de configuracion no pueden estar vacios");
            }
        }
        return configs;
    }

    /** Espera el futuro con el timeout configurado y traduce sus errores a excepciones del dominio. */
    private <T> T await(KafkaFuture<T> future, String operation) {
        try {
            return future.get(properties.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BrokerUnavailableException("Operacion interrumpida al " + operation, ex);
        } catch (java.util.concurrent.TimeoutException ex) {
            throw new BrokerUnavailableException("Timeout al " + operation + " en Kafka", ex);
        } catch (ExecutionException ex) {
            throw translate(operation, ex);
        }
    }

    /** Busca en la cadena de causas el error concreto del broker y lo mapea a HTTP. */
    private static RuntimeException translate(String operation, ExecutionException ex) {
        Throwable cause = ex.getCause() == null ? ex : ex.getCause();
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof UnknownTopicOrPartitionException
                    || current instanceof GroupIdNotFoundException
                    || current instanceof java.util.NoSuchElementException) {
                return new ResourceNotFoundException(
                        "Kafka no encontro el recurso al " + operation + ": " + current.getMessage());
            }
            if (current instanceof TopicExistsException) {
                return new ResourceConflictException("El topico ya existe: " + current.getMessage());
            }
            if (current instanceof RebalanceInProgressException) {
                return new ResourceConflictException(
                        "El consumer group tiene miembros activos: " + current.getMessage());
            }
            if (current instanceof InvalidConfigurationException) {
                return new IllegalArgumentException("Configuracion invalida: " + current.getMessage());
            }
            if (current instanceof org.apache.kafka.common.errors.TimeoutException
                    || current instanceof java.util.concurrent.TimeoutException
                    || current instanceof DisconnectException
                    || current instanceof NetworkException) {
                return new BrokerUnavailableException("Sin conexion con Kafka al " + operation, current);
            }
        }
        if (cause instanceof RetriableException) {
            return new BrokerUnavailableException("Kafka no esta disponible al " + operation, cause);
        }
        if (cause instanceof KafkaException) {
            return new BrokerUnavailableException("Kafka rechazo " + operation + ": " + cause.getMessage(), cause);
        }
        return new BrokerUnavailableException("Falla inesperada al " + operation + ": " + cause.getMessage(), cause);
    }
}
