package storm.repository.com.core.listener;

import org.junit.jupiter.api.Test;
import storm.repository.com.core.adapter.inbound.service.KafkaMessageObserver;
import storm.repository.com.core.dto.MessageType;
import storm.repository.com.core.dto.RepositoryMessageDto;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;
import storm.repository.com.utils.JsonUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageHandlerTest {

    @Test
    void processMessage_buildsResponseAndNotifiesObservers() {
        RepositoryConnectorExecutor executor = new RepositoryConnectorExecutor() {
            @Override
            public String connectorId() {
                return "mongo";
            }

            @Override
            public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
                return Map.of("ok", true, "method", operation.getMethod());
            }
        };

        MessageHandler handler = new MessageHandler(List.of(executor));
        CapturingObserver observer = new CapturingObserver();
        handler.addObserver(observer);

        RepositoryMessageDto request = RepositoryMessageDto.builder()
                .type(MessageType.REQUEST)
                .requestId("req-1")
                .from("service-a")
                .to("repository")
                .connectorId("mongo")
                .config(Map.of("uri", "mongodb://localhost", "database", "test"))
                .operation(RepositoryOperationDto.builder().method("find").collection("users").build())
                .build();

        handler.processMessage(JsonUtil.toJson(request));

        assertEquals(1, observer.messages.size());
        RepositoryMessageDto response = observer.messages.get(0);
        assertEquals(MessageType.RESPONSE, response.getType());
        assertEquals("req-1", response.getRequestId());
        assertEquals("req-1", response.getCorrelationId());
        assertEquals("repository", response.getFrom());
        assertEquals("service-a", response.getTo());
        assertEquals("OK", response.getStatus());
        assertNotNull(response.getData());
    }

    @Test
    void processMessage_whenResponsePassesThrough() {
        MessageHandler handler = new MessageHandler(List.of());
        CapturingObserver observer = new CapturingObserver();
        handler.addObserver(observer);

        RepositoryMessageDto response = RepositoryMessageDto.builder()
                .type(MessageType.RESPONSE)
                .requestId("req-2")
                .from("repository")
                .to("service-a")
                .status("OK")
                .build();

        handler.processMessage(JsonUtil.toJson(response));

        assertEquals(1, observer.messages.size());
        assertEquals("req-2", observer.messages.get(0).getRequestId());
        assertEquals(MessageType.RESPONSE, observer.messages.get(0).getType());
    }

    @Test
    void processMessage_whenConnectorMissingSetsError() {
        MessageHandler handler = new MessageHandler(List.of());
        CapturingObserver observer = new CapturingObserver();
        handler.addObserver(observer);

        RepositoryMessageDto request = RepositoryMessageDto.builder()
                .type(MessageType.REQUEST)
                .requestId("req-3")
                .from("service-a")
                .to("repository")
                .connectorId("missing")
                .config(Map.of("uri", "mongodb://localhost", "database", "test"))
                .operation(RepositoryOperationDto.builder().method("find").collection("users").build())
                .build();

        handler.processMessage(JsonUtil.toJson(request));

        assertEquals(1, observer.messages.size());
        RepositoryMessageDto response = observer.messages.get(0);
        assertEquals("ERROR", response.getStatus());
        assertNotNull(response.getError());
        assertTrue(response.getError().contains("Unsupported connectorId"));
    }

    @Test
    void processMessage_whenEmptyMessageSendsErrorResponse() {
        MessageHandler handler = new MessageHandler(List.of());
        CapturingObserver observer = new CapturingObserver();
        handler.addObserver(observer);

        handler.processMessage("   ");

        assertEquals(1, observer.messages.size());
        RepositoryMessageDto response = observer.messages.get(0);
        assertEquals(MessageType.RESPONSE, response.getType());
        assertEquals("ERROR", response.getStatus());
        assertEquals("Empty message", response.getError());
    }

    @Test
    void processMessage_whenInvalidJsonSendsErrorResponse() {
        MessageHandler handler = new MessageHandler(List.of());
        CapturingObserver observer = new CapturingObserver();
        handler.addObserver(observer);

        handler.processMessage("{invalid-json");

        assertEquals(1, observer.messages.size());
        RepositoryMessageDto response = observer.messages.get(0);
        assertEquals(MessageType.RESPONSE, response.getType());
        assertEquals("ERROR", response.getStatus());
        assertEquals("Invalid JSON payload", response.getError());
    }

    private static class CapturingObserver implements KafkaMessageObserver {
        private final List<RepositoryMessageDto> messages = new ArrayList<>();

        @Override
        public void onMessageReceived(RepositoryMessageDto message) {
            messages.add(message);
        }
    }
}
