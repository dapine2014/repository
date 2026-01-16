package storm.repository.com.core.adapter.inbound.service;


import storm.repository.com.core.dto.MessageResponceDto;

public interface KafkaMessageObserver {
    void onMessageReceived(MessageResponceDto message);
}
