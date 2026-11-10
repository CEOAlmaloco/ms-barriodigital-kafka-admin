package cl.duoc.barriodigital.kafkaadmin.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import java.util.Map;
import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** OpenAPI de kafka-admin: ejemplos por endpoint y códigos de error del contrato. */
@Configuration
public class KafkaOpenApi {

    static final String CREATE_TOPIC_EXAMPLE = """
            {"name":"demo.events","partitions":3,"replicationFactor":3,"configs":{"retention.ms":"604800000","cleanup.policy":"delete"}}""";
    static final String UPDATE_CONFIGS_EXAMPLE = """
            {"configs":{"retention.ms":"259200000"}}""";
    static final String UPDATE_PARTITIONS_EXAMPLE = """
            {"partitions":6}""";
    static final String RESET_OFFSETS_EXAMPLE = """
            {"topic":"requests.events","strategy":"EARLIEST","dryRun":true}""";
    static final String REPLAY_OFFSETS_EXAMPLE = """
            {"offsets":[{"partition":1,"offset":2040}]}""";

    @Bean
    OpenAPI kafkaAdminApi() {
        return new OpenAPI().info(new Info()
                .title("kafka-admin")
                .version("v1")
                .description("Administracion de topicos, consumer groups y DLT. "
                        + "Exige X-Internal-Token y rol Admin en X-User-Roles."));
    }

    @Bean
    OperationCustomizer kafkaOperations() {
        return (operation, handler) -> {
            String method = handler.getMethod().getName();
            Class<?> type = handler.getBeanType();
            if (type == TopicController.class && "create".equals(method)) {
                example(operation, CREATE_TOPIC_EXAMPLE);
            } else if (type == TopicController.class && "updateConfigs".equals(method)) {
                example(operation, UPDATE_CONFIGS_EXAMPLE);
            } else if (type == TopicController.class && "updatePartitions".equals(method)) {
                example(operation, UPDATE_PARTITIONS_EXAMPLE);
            } else if (type == ConsumerGroupController.class && "reset".equals(method)) {
                example(operation, RESET_OFFSETS_EXAMPLE);
            } else if (type == DltController.class && "replay".equals(method)) {
                example(operation, REPLAY_OFFSETS_EXAMPLE);
            }
            codes(operation.getResponses(), type, method);
            successCode(operation.getResponses(), type, method);
            return operation;
        };
    }

    private static void successCode(ApiResponses responses, Class<?> type, String method) {
        ApiResponse ok = responses.remove("200");
        if (ok == null) {
            return;
        }
        if (type == TopicController.class && "create".equals(method)) {
            ok.setDescription("Creado");
            responses.addApiResponse("201", ok);
            return;
        }
        if (type == TopicController.class && "delete".equals(method)) {
            ok.setDescription("Sin cuerpo");
            responses.addApiResponse("204", ok);
            return;
        }
        responses.addApiResponse("200", ok);
    }

    private static void example(io.swagger.v3.oas.models.Operation operation, String json) {
        if (operation.getRequestBody() == null || operation.getRequestBody().getContent() == null) {
            return;
        }
        MediaType media = operation.getRequestBody().getContent().get("application/json");
        if (media != null) {
            media.setExample(json);
        }
    }

    private static void codes(ApiResponses responses, Class<?> type, String method) {
        responses.addApiResponse("401", error(401, SecurityGateFilter.UNAUTHORIZED_MESSAGE));
        responses.addApiResponse("403", error(403, AdminCaller.FORBIDDEN_MESSAGE));
        responses.addApiResponse("503", error(503, "Kafka no esta disponible"));
        if ("create".equals(method) || "updateConfigs".equals(method) || "updatePartitions".equals(method)
                || "reset".equals(method) || "replay".equals(method) || "list".equals(method)
                || "detail".equals(method) || "delete".equals(method)) {
            if (!"list".equals(method) || type == DltController.class) {
                responses.addApiResponse("400", error(400, "El cuerpo no cumple el formato"));
            }
        }
        if ((type == TopicController.class && ("detail".equals(method) || "delete".equals(method)
                || "updateConfigs".equals(method) || "updatePartitions".equals(method)))
                || (type == ConsumerGroupController.class && ("detail".equals(method) || "reset".equals(method)))) {
            responses.addApiResponse("404", error(404, "No existe"));
        }
        if ((type == TopicController.class && ("create".equals(method) || "delete".equals(method)
                || "updateConfigs".equals(method) || "updatePartitions".equals(method)))
                || (type == ConsumerGroupController.class && "reset".equals(method))) {
            responses.addApiResponse("409", error(409, "Conflicto"));
        }
    }

    private static ApiResponse error(int status, String message) {
        MediaType media = new MediaType();
        media.setExample(Map.of(
                "status", status,
                "error", switch (status) {
                    case 400 -> "Bad Request";
                    case 401 -> "Unauthorized";
                    case 403 -> "Forbidden";
                    case 404 -> "Not Found";
                    case 409 -> "Conflict";
                    case 503 -> "Service Unavailable";
                    default -> "Error";
                },
                "message", message,
                "path", "/api/kafka/topics"));
        Content content = new Content().addMediaType("application/json", media);
        return new ApiResponse().description(message).content(content);
    }
}
