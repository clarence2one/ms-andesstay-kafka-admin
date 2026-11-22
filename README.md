# ms-andesstay-kafka-admin

Microservicio administrador de **Kafka** del proyecto **AndesStay** (caso DSY1107 – Desarrollo Cloud Native I, Duoc UC): expone una API REST para **crear, listar, describir, modificar configuraciones y eliminar tópicos**, y para **listar/describir consumer groups con offsets y lag**, sin entrar a la consola del broker.

## Rol en la arquitectura

```
Angular / Postman → API Gateway → ms-andesstay-bff → ms-andesstay-kafka-admin ──Kafka AdminClient──► Kafka (EC2 infra)
```

- No aloja Kafka: el broker sigue en la infraestructura compartida (`andesstay-infra`). Este servicio se conecta a `KAFKA_BOOTSTRAP_SERVERS` y lo administra con `AdminClient` de `kafka-clients` (traído por `spring-boot-starter-kafka`).
- El BFF le reenvía todo lo que llega a `/api/kafka/**`, y ambos exigen el rol **Admin** del JWT de Azure AD (defensa en profundidad, igual que el resto de los microservicios). `/actuator/health` e `/actuator/info` son públicos.
- **Protege la topología del sistema**: `reservations.events`, `reservations.events.DLT` y `audit.timeline` no se pueden eliminar ni reconfigurar desde la API (`409`), para que una llamada de administración no corte los eventos. La lista está en `application.yml` (`andesstay.kafka-admin.protected-topics`).
- No usa base de datos (sin ojdbc ni JPA).

## Relación con la pauta DSY1107

- **EP5 – indicador 3 (administración de tópicos)**: creación, listado, descripción, modificación incremental de configuraciones y eliminación de tópicos.
- **EP5 – indicador 9 (monitoreo de consumidores)**: inspección de consumer groups con `currentOffset`, `logEndOffset` y `lag`.
- **EP6 – administración y monitoreo**: la API cubre la operación del cluster (topología y grupos de consumidores) y el reset de offsets para recovering ante incidentes, con métricas de lag según la unidad **3.3.1**: `Lag = LogEndOffset - CurrentOffset`.

## Diseño

| Capa | Clases | Responsabilidad |
|---|---|---|
| Web | `TopicController`, `ConsumerGroupController` | Rutas REST, `@PreAuthorize("hasRole('Admin')")`, validación de entrada, códigos HTTP. No conocen `AdminClient`. |
| DTOs | `web/dto` (`CreateTopicRequest`, `UpdateTopicConfigRequest`, `ResetOffsetRequest`, `OffsetTarget`, respuestas) | Contrato de la API con Bean Validation. Respuestas como records. |
| Servicio | `KafkaAdminService` | **Toda** la lógica: espera los futures del `AdminClient`, construye `NewTopic`, usa `incrementalAlterConfigs` con `SET`, calcula lag y traduce los errores del broker a excepciones del dominio. |
| Config | `KafkaAdminConfig`, `KafkaAdminProperties`, `SecurityConfig` | Bean `AdminClient` con timeouts configurables, protección de tópicos y seguridad JWT. |
| Errores | `GlobalExceptionHandler` | JSON uniforme `{timestamp, status, message, errors}` para 400/404/409/503. |

## Endpoints

Todos requieren `Authorization: Bearer <access_token>` con el rol `Admin` (excepto health/info).

| Método | Ruta | Descripción | Respuestas |
|---|---|---|---|
| `POST` | `/api/kafka/topics` | Crea un tópico | `201` + `Location` · `400` · `409` |
| `GET` | `/api/kafka/topics` | Lista tópicos con particiones y réplicas | `200` |
| `GET` | `/api/kafka/topics/{name}` | Describe el tópico con su configuración efectiva | `200` · `404` |
| `PATCH` | `/api/kafka/topics/{name}/configs` | Modifica solo las claves enviadas (`SET` incremental) | `200` · `400` · `404` · `409` (protegido) |
| `DELETE` | `/api/kafka/topics/{name}` | Elimina el tópico | `204` · `404` · `409` (protegido) |
| `GET` | `/api/kafka/consumer-groups` | Lista grupos y estados | `200` |
| `GET` | `/api/kafka/consumer-groups/{group}` | Offsets por partición, lag y miembros | `200` · `404` |
| `POST` | `/api/kafka/consumer-groups/{group}/offsets/reset` | Reinicia offsets a `EARLIEST` o `LATEST` | `200` · `400` · `404` · `409` (miembros activos) |

### Ejemplos curl

```bash
TOKEN="<access_token_con_rol_Admin>"
BASE="http://localhost:8087"

# Crear un tópico de 3 particiones con 1 réplica
curl -X POST "$BASE/api/kafka/topics" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"reportes.pdf","partitions":3,"replicationFactor":1,"configs":{"retention.ms":"3600000"}}'

# Listar y describir
curl -H "Authorization: Bearer $TOKEN" "$BASE/api/kafka/topics"
curl -H "Authorization: Bearer $TOKEN" "$BASE/api/kafka/topics/reportes.pdf"

# Modificar una configuración (no reemplaza las demás)
curl -X PATCH "$BASE/api/kafka/topics/reportes.pdf/configs" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"configs":{"retention.ms":"7200000"}}'

# Eliminar (reservations.events, reservations.events.DLT y audit.timeline responden 409)
curl -X DELETE -H "Authorization: Bearer $TOKEN" "$BASE/api/kafka/topics/reportes.pdf"

# Consumer groups: listar, ver lag y reiniciar offsets
curl -H "Authorization: Bearer $TOKEN" "$BASE/api/kafka/consumer-groups"
curl -H "Authorization: Bearer $TOKEN" "$BASE/api/kafka/consumer-groups/notify-group"
curl -X POST "$BASE/api/kafka/consumer-groups/notify-group/offsets/reset" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"topic":"reservations.events","target":"EARLIEST"}'
```

Respuesta de `GET /api/kafka/consumer-groups/{group}` (lag calculado como `LogEndOffset - CurrentOffset`):

```json
{
  "groupId": "notify-group",
  "state": "STABLE",
  "offsets": [
    { "partition": 0, "currentOffset": 120, "logEndOffset": 125, "lag": 5 }
  ],
  "members": [
    { "memberId": "consumer-1", "clientId": "ms-andesstay-notify", "host": "/10.0.0.4" }
  ]
}
```

### Validación de entrada

| Campo | Regla |
|---|---|
| `name` (tópico) | Obligatorio; letras, dígitos, `.`, `_`, `-` (1–249), empieza por letra o dígito |
| `partitions` | Entero entre 1 y 100 |
| `replicationFactor` | Entero entre 1 y 10 |
| `configs` | No vacío en PATCH; claves y valores no vacíos |
| `target` | `EARLIEST` o `LATEST` (en mayúsculas) |
| `group` | Letras, dígitos, `.`, `_`, `-` (1–200), empieza por letra o dígito |

Un error de validación responde `400` con el detalle por campo:

```json
{ "status": 400, "message": "La solicitud tiene parametros invalidos",
  "errors": { "name": "solo puede tener letras, numeros, '.', '_' o '-' (1 a 249 caracteres) y empezar por letra o numero" } }
```

## Stack

- Spring Boot 4.1.1 / Java 21
- `spring-boot-starter-kafka` (`AdminClient`), `spring-boot-starter-webmvc`, `spring-boot-starter-json`
- Spring Security OAuth2 Resource Server (JWT de Azure AD, claim `roles`)
- Bean Validation · Spring Boot Actuator

## Configuración

| Variable | Descripción | Default |
|---|---|---|
| `SERVER_PORT` | Puerto HTTP | `8087` |
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers Kafka (bootstrap servers) | `localhost:9092` |
| `AZURE_ISSUER_URI` | Issuer del JWT de Azure AD | tenant del caso (`fbf645d5-…`) |
| `AZURE_API_AUDIENCE` | Audiencia esperada del JWT | `api://d6959b0c-9ec2-432e-bddb-54bd0e83ace9` |
| `andesstay.kafka-admin.request-timeout-ms` | Timeout de cada request del AdminClient | `10000` |
| `andesstay.kafka-admin.default-api-timeout-ms` | Timeout por defecto de las APIs de Kafka | `10000` |
| `andesstay.kafka-admin.protected-topics` | Tópicos que no se pueden borrar/reconfigurar | `reservations.events`, `reservations.events.DLT`, `audit.timeline` |

## Cómo ejecutarlo en local

Requiere Java 21 y un Kafka levantado en `KAFKA_BOOTSTRAP_SERVERS` (ver `andesstay-infra`).

```bash
./mvnw spring-boot:run
./mvnw test
```

Pruebas (no necesitan broker ni Azure AD):

- `MsAndesstayKafkaAdminApplicationTests`: carga del contexto con el `JwtDecoder` simulado.
- `KafkaAdminControllersTest`: `401` sin token, `403` con otro rol, validaciones, ciclo completo de tópicos y consumer groups con lag, y traducción de 404/409/503.

## Repos relacionados

[frontend-andesstay](https://github.com/MarCOCO999/frontend-andesstay) · [ms-andesstay-bff](https://github.com/MarCOCO999/ms-andesstay-bff) · [ms-andesstay-reservations](https://github.com/MarCOCO999/ms-andesstay-reservations) · [ms-andesstay-notify](https://github.com/MarCOCO999/ms-andesstay-notify) · [ms-andesstay-audit](https://github.com/MarCOCO999/ms-andesstay-audit) · [ms-andesstay-rabbit-admin](https://github.com/MarCOCO999/ms-andesstay-rabbit-admin) · [andesstay-infra](https://github.com/MarCOCO999/andesstay-infra)
