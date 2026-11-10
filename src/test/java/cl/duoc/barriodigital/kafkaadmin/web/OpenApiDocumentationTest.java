package cl.duoc.barriodigital.kafkaadmin.web;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.barriodigital.kafkaadmin.service.KafkaRecordClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(properties = {
        "barriodigital.security.internal-token=test-token",
        "management.health.kafka.enabled=false"
})
@AutoConfigureMockMvc
class OpenApiDocumentationTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private KafkaRecordClient recordClient;

    @Test
    void laSpecListaEndpointsEjemplosYCodigos() throws Exception {
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}'].delete").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}/configs'].patch").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics/{name}/partitions'].patch").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/consumer-groups/{groupId}/offsets/reset'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/dlt/{name}/messages'].get").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/dlt/{name}/replay'].post").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].post.requestBody.content['application/json'].example")
                        .value(KafkaOpenApi.CREATE_TOPIC_EXAMPLE))
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].post.responses['201']").exists())
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].get.responses['401'].description")
                        .value(SecurityGateFilter.UNAUTHORIZED_MESSAGE))
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].get.responses['403'].description")
                        .value(AdminCaller.FORBIDDEN_MESSAGE))
                .andExpect(jsonPath("$.paths['/api/kafka/topics'].get.responses['503'].description")
                        .value("Kafka no esta disponible"));

        MvcResult ui = mvc.perform(get("/swagger-ui.html")).andReturn();
        int status = ui.getResponse().getStatus();
        assertTrue(status == 200 || status == 302 || status == 301, "swagger-ui.html respondio " + status);
        if (status == 302 || status == 301) {
            String location = ui.getResponse().getHeader("Location");
            String path = location == null ? "" : location.replaceFirst("https?://[^/]+", "");
            mvc.perform(get(path)).andExpect(status().isOk());
        }
    }
}
