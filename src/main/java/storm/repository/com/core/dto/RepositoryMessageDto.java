package storm.repository.com.core.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RepositoryMessageDto {
    private MessageType type;
    private String requestId;
    private String correlationId;
    private String from;
    private String to;
    private String connectorId;
    private Map<String, String> config;
    private RepositoryOperationDto operation;
    private Object data;
    private String status;
    private String error;
}
