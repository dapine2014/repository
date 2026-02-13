package storm.repository.com.core.listener;



import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import storm.repository.com.core.adapter.inbound.service.KafkaMessageObserver;
import storm.repository.com.core.config.RepositoryTargetRegistry;
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
    private final RepositoryTargetRegistry repositoryTargetRegistry;

    public MessageHandler(List<RepositoryConnectorExecutor> executors) {
        this(executors, null);
    }

    @Autowired
    public MessageHandler(List<RepositoryConnectorExecutor> executors, RepositoryTargetRegistry repositoryTargetRegistry) {
        this.executorsById = new HashMap<>();
        this.repositoryTargetRegistry = repositoryTargetRegistry;
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
        if (message == null || message.isBlank()) {
            notify(buildErrorResponse(null, "Empty message"));
            return;
        }
        RepositoryMessageDto payload;
        try {
            payload = fromJson(message, RepositoryMessageDto.class);
        } catch (RuntimeException ex) {
            log.error("Failed to parse message as JSON", ex);
            notify(buildErrorResponse(null, "Invalid JSON payload"));
            return;
        }
        if (payload.getType() == null || payload.getType() == MessageType.REQUEST) {
            RepositoryMessageDto effectivePayload;
            try {
                effectivePayload = resolveRequestTarget(payload);
            } catch (IllegalArgumentException ex) {
                notify(buildErrorResponse(payload, ex.getMessage()));
                return;
            }
            String validationError = validateRequest(effectivePayload);
            if (validationError != null) {
                notify(buildErrorResponse(effectivePayload, validationError));
                return;
            }
            createAndSendResponseMessage(effectivePayload);
        } else {
            notify(payload);
        }

    }

    public void logReceivedMessage(String message){
        log.info("Message received: {}", message);
    }

    private RepositoryMessageDto resolveRequestTarget(RepositoryMessageDto data) {
        String targetRef = firstNonBlank(data.getConfigRef(), data.getRepositoryId());
        if (targetRef == null) {
            return data;
        }
        if (repositoryTargetRegistry == null) {
            throw new IllegalArgumentException("Repository target registry is not configured");
        }
        if (data.getConfig() != null && !data.getConfig().isEmpty()) {
            throw new IllegalArgumentException("config must be omitted when configRef/repositoryId is provided");
        }
        RepositoryTargetRegistry.ResolvedTarget resolvedTarget = repositoryTargetRegistry.resolve(targetRef);
        if (resolvedTarget == null) {
            throw new IllegalArgumentException("Unable to resolve repository target: " + targetRef);
        }
        String connectorId = firstNonBlank(data.getConnectorId(), resolvedTarget.connectorId());
        if (connectorId == null) {
            throw new IllegalArgumentException("Missing connectorId for repository target: " + targetRef);
        }
        data.setConnectorId(connectorId);
        data.setConfig(resolvedTarget.config());
        return data;
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

    private String validateRequest(RepositoryMessageDto data) {
        if (data == null) {
            return "Missing message body";
        }
        if (data.getRequestId() == null || data.getRequestId().isBlank()) {
            return "Missing requestId";
        }
        if (data.getFrom() == null || data.getFrom().isBlank()) {
            return "Missing from";
        }
        if (data.getTo() == null || data.getTo().isBlank()) {
            return "Missing to";
        }
        if (data.getConnectorId() == null || data.getConnectorId().isBlank()) {
            return "Missing connectorId";
        }
        if (data.getConfig() == null || data.getConfig().isEmpty()) {
            return "Missing config";
        }
        if (data.getOperation() == null) {
            return "Missing operation";
        }
        return null;
    }

    private String firstNonBlank(String primary, String secondary) {
        if (primary != null && !primary.isBlank()) {
            return primary;
        }
        if (secondary != null && !secondary.isBlank()) {
            return secondary;
        }
        return null;
    }

    private RepositoryMessageDto buildErrorResponse(RepositoryMessageDto data, String errorMessage) {
        return RepositoryMessageDto.builder()
                .type(MessageType.RESPONSE)
                .requestId(data == null ? null : data.getRequestId())
                .correlationId(data == null ? null : data.getCorrelationId())
                .from(data == null ? "repository" : data.getTo())
                .to(data == null ? null : data.getFrom())
                .connectorId(data == null ? null : data.getConnectorId())
                .status("ERROR")
                .error(errorMessage)
                .build();
    }
}
