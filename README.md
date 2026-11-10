# ms-barriodigital-kafka-admin

Administración de topics y consumer groups de Kafka de **BarrioDigital**.

Puerto: **8085**

## Arranque

El cluster de Kafka tiene que estar levantado. Este servicio no lo incluye; en local se espera un broker en `localhost:9092` y en EC2 el cluster de EC2-Kafka.

```powershell
copy .env.example .env
.\mvnw.cmd spring-boot:run
```

```powershell
curl http://localhost:8085/actuator/health
```

Con los brokers prendidos y `KAFKA_BOOTSTRAP_SERVERS` correcto, la respuesta es `{"status":"UP"}`. Sin brokers, el health de Kafka responde `DOWN`.

## Variables (`.env`, no subir a git)

| Variable | Qué va |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers separados por coma. En local, `localhost:9092`. En EC2, DNS o IP privada de EC2-Kafka. |
| `INTERNAL_TOKEN` | Valor del header `X-Internal-Token`. Sin él, `/api/kafka/**` responde 401. |

Este servicio no ve el JWT: lo llama el BFF. Tras el token interno exige rol Admin en `X-User-Roles` (403 si falta).

## Tópicos del sistema (EP3-18)

Al arrancar, `SystemTopicsInitializer` crea en Kafka los tópicos declarados en
`barriodigital.kafka.topics.*` de `application.yml` (los 5 del contrato EP3-01:
`requests.events`, `audit.timeline` y las 3 DLT). Reglas:

- Solo se crean los que falten. Si uno ya existe con configs distintas, se
  registra un WARN y **no se modifica**.
- Las DLT se crean con `message.timestamp.type=LogAppendTime`.
- Si Kafka no responde al arrancar, el servicio igual levanta y reintenta la
  creación cada `barriodigital.kafka.topics-init.retry-ms` (10 s por defecto).
  Mientras tanto `KafkaAdminService.isAvailable()` devuelve `false` y los
  endpoints que dependan de Kafka responderán 503.
- `KafkaAdminService` es el único punto que usa `AdminClient`; ningún
  controlador toca clases de Kafka.

## Tópicos: consulta, alta, configuraciones y baja (EP3-19)

Endpoints bajo `/api/kafka/topics` (EP3-03, secciones 2 a 5):

| Método | Ruta | Éxito |
|---|---|---|
| GET | `/api/kafka/topics` | 200, lista con `system` (sin tópicos internos) |
| GET | `/api/kafka/topics/{name}` | 200, particiones (líder, réplicas, ISR) y configs con `value`, `source` y `editable` |
| POST | `/api/kafka/topics` | 201 con el detalle |
| PATCH | `/api/kafka/topics/{name}/configs` | 200 con el detalle; cambio parcial vía `incrementalAlterConfigs` (`SET`) |
| PATCH | `/api/kafka/topics/{name}/partitions` | 200 con el detalle; solo aumenta, máx. 12 |
| DELETE | `/api/kafka/topics/{name}` | 204 |

Reglas principales:

- Nombre: `^[A-Za-z0-9._-]{1,249}$`, sin prefijo `__`, sin `.` ni `..`.
- Alta: `partitions` 1-12, `replicationFactor` 1-3 y ≤ brokers del clúster.
  409 si ya existe, si termina en `.DLT` o si choca con un existente al
  cambiar `.` por `_` (revisado contra el listado antes de llamar a Kafka).
- Configs editables: `retention.ms`, `cleanup.policy`, `min.insync.replicas`,
  `min.compaction.lag.ms` y `segment.ms`, con los rangos de EP3-03 3.3.
  En los tópicos del sistema solo `retention.ms` es editable (409 el resto);
  `message.timestamp.type` nunca es editable. Un valor `null` responde 400.
- Baja: 409 en tópicos del sistema o si algún consumer group con miembros
  tiene particiones asignadas del tópico.

## Consumer groups, lag y reset de offsets (EP3-20)

Endpoints bajo `/api/kafka/consumer-groups` (EP3-03, sección 6):

- `GET /api/kafka/consumer-groups` — grupos del broker más `report-group` y
  `audit-group`, declarados en `barriodigital.kafka.groups.*`. Si el broker los
  borró aparecen con `state: DEAD` y `members: 0`.
- `GET /api/kafka/consumer-groups/{groupId}` — offset confirmado, offset final
  y lag por partición. `lagBasis` es `COMMITTED` con offset confirmado;
  `EARLIEST` en grupos del sistema sin offset (lag real con
  `auto-offset-reset=earliest`); `null` en otros grupos sin offset. El lag es
  una distancia de offsets, no una cantidad exacta de mensajes (en
  `audit.timeline`, compactado, puede ser mayor que lo que falta leer).
- `POST /api/kafka/consumer-groups/{groupId}/offsets/reset` — `topic`,
  `strategy` (`EARLIEST`/`LATEST`/`TIMESTAMP`) y `dryRun` obligatorios;
  `timestamp` obligatorio con `TIMESTAMP` y no puede ser futuro. El dry run
  aplica las mismas validaciones (incluidos los 409) y responde la misma forma.

### Restricciones de los grupos del sistema

No se permite mover un offset hacia adelante (salta eventos: KPIs de report y
la historia de audit en Oracle):

| Estrategia | Con offset confirmado | Sin offset confirmado |
|---|---|---|
| `EARLIEST` | Permitido | Permitido |
| `LATEST` | 409 | 409 |
| `TIMESTAMP` | Permitido si no supera el confirmado | 409 |

Otros grupos: 404 si no existen, 409 si no tienen offsets confirmados en el
tópico. Con miembros activos siempre es 409.

### Procedimiento operativo del reset

En una ventana de mantenimiento:

1. Detener el consumidor (por ejemplo, `docker compose stop report`), para que
   el grupo quede sin miembros.
2. Ejecutar el reset con `"dryRun": true` y revisar los offsets resultantes.
3. Ejecutarlo con `"dryRun": false`.
4. Levantar el consumidor y seguir el lag en Kafka UI hasta que llegue a 0.

## DLT: listado y replay selectivo (EP3-21)

Endpoints bajo `/api/kafka/dlt` (EP3-03, sección 7). Solo aceptan las DLT del
sistema (`*.DLT` de `barriodigital.kafka.topics`); cualquier otro nombre responde
400.

| Método | Ruta | Éxito |
|---|---|---|
| GET | `/api/kafka/dlt/{name}/messages` | 200, mensajes por partición |
| POST | `/api/kafka/dlt/{name}/replay` | 200 con el estado de cada mensaje |

### Listado

Lee con un consumidor asignado a las particiones, **sin** `group.id` ni offsets
confirmados, con tope de 5 s. Cada mensaje incluye `key`, `eventId`, headers de
diagnóstico (excepción, causa, mensaje, tópico/partición/offset/grupo de origen)
y dos fechas: llegada a la DLT y `kafka_dlt-original-timestamp`. Sin stacktrace.

Paginación por partición: `partition`, `fromOffset` (exige `partition`) y
`limit` (por partición, 20 por defecto, máx. 100). Un `fromOffset` anterior a
la retención se ajusta al primer offset disponible. La respuesta trae
`nextOffset` por partición.

### Replay

Cuerpo: `offsets` (lista de `{partition, offset}`) **o** `eventIds`, uno de los
dos y nunca ambos (400). Máximo 100. Los mensajes sin `eventId` solo se eligen
por offset. Un `eventId` repetido en la DLT se reenvía una sola vez (la
ocurrencia más reciente).

Publicación:

- va al tópico original, a la partición de `kafka_dlt-original-partition`, con
  la key y el valor originales y la hora actual como timestamp;
- quita los headers `kafka_dlt-*` y agrega `x-replayed-from-dlt` y
  `x-replayed-at`;
- espera la confirmación de cada mensaje antes de seguir;
- publica en orden de partición y offset, y se detiene ante la primera falla.

La respuesta 200 trae el estado de cada mensaje (`REENVIADO`, `OMITIDO`,
`FALLIDO`, `NO_INTENTADO`), aunque haya fallidos. 503 si no hay ningún broker
disponible, antes de intentar nada.

Tope de 23 s para toda la operación (menor que el timeout de 25 s del BFF). Al
agotarse, lo pendiente queda `NO_INTENTADO`. Un `eventId` que no se alcanzó a
buscar queda `NO_INTENTADO` con motivo `busqueda agotada`; `OMITIDO` queda solo
para lo que la búsqueda completa no encontró. Antes de cada envío, si quedan
menos de 15 s del tope, el mensaje no se envía y queda `NO_INTENTADO`. Un
envío sin confirmación queda `FALLIDO` con motivo `sin confirmacion` y detiene
la operación.

## Auditoría en audit.timeline (EP3-23)

Tras una operación exitosa (alta/cambio/baja de tópico, reset real de offsets,
replay de DLT), kafka-admin encola un evento en `audit.timeline` (productor
`acks=all` + idempotencia, un hilo, cola 1000). El `dryRun` no publica. Si Kafka
falla o el servicio se apaga con eventos pendientes, queda
`ERROR evento-auditoria-no-publicado` con el JSON del evento. Actor desde
`X-User-Id|Name|Roles` y `X-Source-Ip`.

## Validación, seguridad, Swagger y tests (EP3-22)

- DTOs con `jakarta.validation` (`@NotNull` en `dryRun` Boolean, rangos de
  particiones/réplicas, formato de nombre). Orden de errores: 401 → 403 → 400 →
  404 → 409. Kafka sin respuesta a tiempo: 503 con el mismo cuerpo.
- `AdminClient` con `default.api.timeout.ms=5000`.
- Swagger: `/swagger-ui.html` y `/v3/api-docs` con ejemplos por endpoint.
- `.\mvnw.cmd test` corre sin un Kafka real (AdminClient mockeado).
