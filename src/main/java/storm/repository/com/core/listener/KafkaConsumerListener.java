package storm.repository.com.core.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Configuration
public class KafkaConsumerListener {
    @Value("${app.kafka.topic.repository-query}")
    private String topics;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    private final MessageHandler messageHandler;

    @Autowired
    public KafkaConsumerListener(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @KafkaListener(topics = "${app.kafka.topic.repository-query}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessage(String message) {
        messageHandler.processMessage(message);
    }
}
