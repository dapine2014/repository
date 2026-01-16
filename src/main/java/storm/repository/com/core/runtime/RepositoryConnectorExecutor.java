package storm.repository.com.core.runtime;

import storm.repository.com.core.dto.RepositoryOperationDto;

import java.util.Map;

public interface RepositoryConnectorExecutor {
    String connectorId();

    Object execute(RepositoryOperationDto operation, Map<String, String> config);
}
