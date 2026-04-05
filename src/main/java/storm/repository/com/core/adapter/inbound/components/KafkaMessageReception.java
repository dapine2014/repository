package storm.repository.com.core.adapter.inbound.components;



import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.adapter.inbound.service.KafkaMessageObserver;
import storm.repository.com.core.dto.RepositoryMessageDto;

import static storm.repository.com.utils.JsonUtil.toJson;


@Slf4j
@Component
public class KafkaMessageReception implements KafkaMessageObserver {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String responseTopic;

    public KafkaMessageReception(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic.repository-response}") String responseTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.responseTopic = responseTopic;
    }


    @Override
    public void onMessageReceived(RepositoryMessageDto message) {
        String payload = toJson(message);
        kafkaTemplate.send(responseTopic, payload);
    }
}
