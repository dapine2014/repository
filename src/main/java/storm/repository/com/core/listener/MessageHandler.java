package storm.repository.com.core.listener;



import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import storm.repository.com.core.adapter.inbound.service.KafkaMessageObserver;
import storm.repository.com.core.dto.MessageType;
import storm.repository.com.core.dto.RepositoryMessageDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static storm.repository.com.utils.JsonUtil.fromJson;


@Slf4j
@Service
public class MessageHandler {
    private final List<KafkaMessageObserver> observers = new ArrayList<>();
    private final Map<String, RepositoryConnectorExecutor> executorsById;

    @Autowired
    public MessageHandler(List<RepositoryConnectorExecutor> executors) {
        this.executorsById = new HashMap<>();
        for (RepositoryConnectorExecutor executor : executors) {
            this.executorsById.put(executor.connectorId(), executor);
        }
    }

    public void addObserver(KafkaMessageObserver observer) {
        this.observers.add(observer);
    }

    public void notify(RepositoryMessageDto message) {
        for (KafkaMessageObserver observer : observers) {
            observer.onMessageReceived(message);
        }
    }

    public void processMessage(String message) {
        logReceivedMessage(message);
        RepositoryMessageDto payload = fromJson(message, RepositoryMessageDto.class);
        if (payload.getType() == null || payload.getType() == MessageType.REQUEST) {
            createAndSendResponseMessage(payload);
        } else {
            notify(payload);
        }

    }

    public void logReceivedMessage(String message){
        log.info("Message received: {}", message);
    }

    private void createAndSendResponseMessage(RepositoryMessageDto data){
        RepositoryMessageDto response = RepositoryMessageDto.builder()
                .type(MessageType.RESPONSE)
                .requestId(data.getRequestId())
                .correlationId(data.getCorrelationId() == null ? data.getRequestId() : data.getCorrelationId())
                .from(data.getTo())
                .to(data.getFrom())
                .connectorId(data.getConnectorId())
                .build();

        try {
            RepositoryConnectorExecutor executor = executorsById.get(data.getConnectorId());
            if (executor == null) {
                throw new IllegalArgumentException("Unsupported connectorId: " + data.getConnectorId());
            }
            Object result = executor.execute(data.getOperation(), data.getConfig());
            response.setStatus("OK");
            response.setData(result);
        } catch (Exception ex) {
            response.setStatus("ERROR");
            response.setError(ex.getMessage());
            log.error("Failed to process request {}", data.getRequestId(), ex);
        }

        notify(response);

    }
}
