package storm.repository.com.connectors.postgres;

import org.junit.jupiter.api.Test;
import storm.repository.com.core.dto.RepositoryOperationDto;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresConnectorExecutorSsmDispatchTest {

    private final PostgresConnectorExecutor executor = new PostgresConnectorExecutor();

    @Test
    void executeConAccesoTipoAwsSsmDespachaHaciaSsmTunnelManager() {
        Map<String, String> config = new HashMap<>();
        config.put("accesoTipo", "AWS_SSM");
        config.put("host", "10.0.1.20");
        config.put("port", "5432");
        config.put("database", "test");
        config.put("username", "u");
        config.put("password", "p");
        // Faltan region/ssmInstanceId/ssmAccessKeyId/ssmSecretAccessKey a propósito.

        RepositoryOperationDto operation = new RepositoryOperationDto();
        operation.setMethod("find");
        operation.setCollection("test");

        assertThatThrownBy(() -> executor.execute(operation, config))
                .hasMessageContaining("AWS SSM");
    }
}
