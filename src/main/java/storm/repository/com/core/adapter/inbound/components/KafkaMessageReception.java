package storm.repository.com.core.adapter.inbound.components;



import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.adapter.inbound.service.KafkaMessageObserver;
import storm.repository.com.core.dto.MessageResponceDto;


@Slf4j
@Component
public class KafkaMessageReception implements KafkaMessageObserver {

    @Override
    public void onMessageReceived(MessageResponceDto message) {

    }
}
