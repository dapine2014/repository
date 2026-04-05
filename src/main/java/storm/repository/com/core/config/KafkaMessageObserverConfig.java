package storm.repository.com.core.config;


import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import storm.repository.com.core.adapter.inbound.components.KafkaMessageReception;
import storm.repository.com.core.listener.MessageHandler;


@Configuration
public class KafkaMessageObserverConfig {


    private final MessageHandler HandlerListener;
    public final KafkaMessageReception messageReception;

    @Autowired
    public KafkaMessageObserverConfig(MessageHandler handlerListener, KafkaMessageReception messageReception) {
        this.HandlerListener = handlerListener;
        this.messageReception = messageReception;
    }

    @PostConstruct
    public void init() {
        HandlerListener.addObserver(messageReception);
    }
}
