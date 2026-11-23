package cl.duoc.barriodigital.kafkaadmin.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springdoc.core.configuration.SpringDocConfiguration;
import org.springdoc.core.configuration.SpringDocSecurityConfiguration;
import org.springdoc.core.properties.SpringDocConfigProperties;
import org.springdoc.webmvc.core.configuration.SpringDocWebMvcConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * contracts/openapi.json es el contrato HTTP publicado del servicio y se genera desde los controladores. Una ruta,
 * un parámetro o un cuerpo que cambia sin actualizar el contrato rompe esta prueba. Para regenerarlo después de
 * revisar el cambio: {@code ./mvnw test -Dtest=OpenApiContractTest -Dopenapi.update=true}.
 */
@WebMvcTest(properties = {
        "springdoc.writer-with-order-by-keys=true",
        "springdoc.writer-with-default-pretty-printer=true"
})
@AutoConfigureMockMvc(addFilters = false)
@ImportAutoConfiguration({
        SpringDocConfiguration.class,
        SpringDocConfigProperties.class,
        SpringDocSecurityConfiguration.class,
        SpringDocWebMvcConfiguration.class
})
@Import({OpenApiContractTest.ContractInfo.class, OpenApiContractTest.MockMissingDependencies.class})
class OpenApiContractTest {

    private static final Path CONTRACT = Path.of("contracts", "openapi.json");
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Autowired
    MockMvc mvc;

    @Test
    void elContratoPublicadoCoincideConLosControladores() throws Exception {
        String body = mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
        ObjectNode generated = (ObjectNode) JSON.readTree(body);
        generated.remove("servers");
        stableOperationIds(generated);

        if (Boolean.getBoolean("openapi.update")) {
            Files.createDirectories(CONTRACT.getParent());
            Files.writeString(CONTRACT, JSON.writeValueAsString(generated) + "\n", StandardCharsets.UTF_8);
        }
        JsonNode published = JSON.readTree(Files.readString(CONTRACT, StandardCharsets.UTF_8));
        assertEquals(published, generated, "Los controladores ya no coinciden con " + CONTRACT
                + ". Revisa el cambio y regenera el contrato con -Dopenapi.update=true.");
    }

    /**
     * springdoc numera al azar los operationId repetidos (proxy_21, proxy_26). Esos se reemplazan por uno único y
     * estable armado con el método y la ruta.
     */
    private static void stableOperationIds(ObjectNode spec) {
        Map<String, Integer> uses = new HashMap<>();
        spec.path("paths").forEach(path -> path.forEach(operation ->
                uses.merge(baseOperationId(operation), 1, Integer::sum)));
        spec.path("paths").fields().forEachRemaining(path -> path.getValue().fields().forEachRemaining(operation -> {
            if (operation.getValue() instanceof ObjectNode node && uses.get(baseOperationId(node)) > 1) {
                String route = path.getKey().replaceAll("[^A-Za-z0-9]+", "_").replaceAll("^_|_$", "");
                node.put("operationId", operation.getKey() + "_" + route);
            }
        }));
    }

    private static String baseOperationId(JsonNode operation) {
        return operation.path("operationId").asText().replaceFirst("_\\d+$", "");
    }

    @TestConfiguration
    static class ContractInfo {
        @Bean
        OpenAPI contractInfo() {
            return new OpenAPI().info(new Info().title("ms-barriodigital-kafka-admin").version("1"));
        }
    }

    /** Solo se prueba la capa HTTP: las dependencias de controladores, filtros e interceptores van como mocks. */
    static class MockMissingDependencies implements BeanFactoryPostProcessor {
        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory factory) {
            for (String name : factory.getBeanDefinitionNames()) {
                String className = factory.getBeanDefinition(name).getBeanClassName();
                if (className == null || !className.startsWith("cl.duoc.barriodigital.")) {
                    continue;
                }
                Class<?> type = ClassUtils.resolveClassName(className, factory.getBeanClassLoader());
                for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                    for (Class<?> parameter : constructor.getParameterTypes()) {
                        mockIfMissing(factory, parameter);
                    }
                }
            }
        }

        private static void mockIfMissing(ConfigurableListableBeanFactory factory, Class<?> type) {
            if (type.isPrimitive() || type.isArray() || type == String.class || Number.class.isAssignableFrom(type)
                    || Boolean.class == type || Iterable.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type)
                    || Optional.class == type || ObjectProvider.class.isAssignableFrom(type)
                    || factory.getBeanNamesForType(type, true, false).length > 0) {
                return;
            }
            factory.registerSingleton("contractMock." + type.getName(), Mockito.mock(type));
        }
    }
}
