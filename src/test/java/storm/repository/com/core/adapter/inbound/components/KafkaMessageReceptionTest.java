package storm.repository.com.core.adapter.inbound.components;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import storm.repository.com.core.dto.MessageType;
import storm.repository.com.core.dto.RepositoryMessageDto;
import storm.repository.com.utils.JsonUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class KafkaMessageReceptionTest {

    @Test
    void onMessageReceived_sendsToKafkaTopic() {
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        KafkaMessageReception reception = new KafkaMessageReception(kafkaTemplate, "response-topic");

        RepositoryMessageDto message = RepositoryMessageDto.builder()
                .type(MessageType.RESPONSE)
                .requestId("req-1")
                .from("repository")
                .to("service-a")
                .status("OK")
                .build();

        reception.onMessageReceived(message);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("response-topic"), payloadCaptor.capture());

        RepositoryMessageDto parsed = JsonUtil.fromJson(payloadCaptor.getValue(), RepositoryMessageDto.class);
        assertEquals("req-1", parsed.getRequestId());
        assertEquals(MessageType.RESPONSE, parsed.getType());
    }
}
