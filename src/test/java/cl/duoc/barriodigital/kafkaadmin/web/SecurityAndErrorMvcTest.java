package cl.duoc.barriodigital.kafkaadmin.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import cl.duoc.barriodigital.kafkaadmin.config.InternalTokenProperties;
import cl.duoc.barriodigital.kafkaadmin.domain.KafkaUnavailableException;
import cl.duoc.barriodigital.kafkaadmin.service.ConsumerGroupService;
import cl.duoc.barriodigital.kafkaadmin.service.DltService;
import cl.duoc.barriodigital.kafkaadmin.service.TopicService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@WebMvcTest(controllers = {TopicController.class, ConsumerGroupController.class, DltController.class})
@Import({SecurityGateFilter.class, ApiExceptionHandler.class, InternalTokenProperties.class})
class SecurityAndErrorMvcTest {

    private static final String TOKEN = "test-internal-token";

    @Autowired
    private MockMvc mvc;

    @Autowired
    private InternalTokenProperties tokenProperties;

    @MockBean
    private TopicService topicService;

    @MockBean
    private ConsumerGroupService consumerGroupService;

    @MockBean
    private DltService dltService;

    @BeforeEach
    void setToken() {
        tokenProperties.setInternalToken(TOKEN);
    }

    @Test
    void sinTokenResponde401YNoLeeRoles() throws Exception {
        mvc.perform(get("/api/kafka/topics").header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.timestamp").isNotEmpty())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"))
                .andExpect(jsonPath("$.message").value(SecurityGateFilter.UNAUTHORIZED_MESSAGE))
                .andExpect(jsonPath("$.path").value("/api/kafka/topics"));
        verify(topicService, never()).listTopics();
    }

    @Test
    void tokenInvalidoResponde401AunqueVengaAdmin() throws Exception {
        mvc.perform(get("/api/kafka/topics")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, "otro")
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isUnauthorized());
        verify(topicService, never()).listTopics();
    }

    @Test
    void conTokenSinAdminResponde403AntesDeValidarElCuerpo() throws Exception {
        mvc.perform(post("/api/kafka/topics")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Funcionario")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{esto no es json}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message").value(AdminCaller.FORBIDDEN_MESSAGE));
        verify(topicService, never()).createTopic(any());
    }

    @Test
    void dryRunAusenteResponde400() throws Exception {
        mvc.perform(post("/api/kafka/consumer-groups/report-group/offsets/reset")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"requests.events\",\"strategy\":\"EARLIEST\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("dryRun es obligatorio"))
                .andExpect(jsonPath("$.path").value("/api/kafka/consumer-groups/report-group/offsets/reset"));
        verify(consumerGroupService, never()).resetOffsets(anyString(), any());
    }

    @Test
    void partitionsFueraDeRangoResponde400() throws Exception {
        mvc.perform(post("/api/kafka/topics")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "ROLE_Admin")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo.events\",\"partitions\":13,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("partitions debe ser un entero de 1 a 12"));
        verify(topicService, never()).createTopic(any());
    }

    @Test
    void topicoInexistenteResponde404() throws Exception {
        when(topicService.describeTopic("no.existe"))
                .thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND, "El topico no existe"));

        mvc.perform(get("/api/kafka/topics/no.existe")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("El topico no existe"))
                .andExpect(jsonPath("$.path").value("/api/kafka/topics/no.existe"));
    }

    @Test
    void topicoDelSistemaResponde409() throws Exception {
        doThrow(new ResponseStatusException(HttpStatus.CONFLICT, "El topico pertenece al sistema"))
                .when(topicService).deleteTopic("requests.events");

        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .delete("/api/kafka/topics/requests.events")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("El topico pertenece al sistema"));
    }

    @Test
    void timeoutDeKafkaResponde503() throws Exception {
        when(topicService.listTopics()).thenThrow(new KafkaUnavailableException("timeout"));

        mvc.perform(get("/api/kafka/topics")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.message").value("Kafka no esta disponible"))
                .andExpect(jsonPath("$.path").value("/api/kafka/topics"));
    }

    @Test
    void adminConTokenListaTopicos() throws Exception {
        when(topicService.listTopics()).thenReturn(java.util.List.of());

        mvc.perform(get("/api/kafka/topics")
                        .header(SecurityGateFilter.HEADER_INTERNAL_TOKEN, TOKEN)
                        .header(AdminCaller.HEADER_USER_ROLES, "Admin"))
                .andExpect(status().isOk());
        verify(topicService).listTopics();
        verify(dltService, never()).listMessages(anyString(), any(), any(), anyInt());
    }
}
