package storm.repository.com.core.adapter.inbound.service;


import storm.repository.com.core.dto.RepositoryMessageDto;

public interface KafkaMessageObserver {
    void onMessageReceived(RepositoryMessageDto message);
}
