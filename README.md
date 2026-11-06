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

Este servicio no ve el JWT: lo llama el BFF.

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
