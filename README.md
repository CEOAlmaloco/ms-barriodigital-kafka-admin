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
