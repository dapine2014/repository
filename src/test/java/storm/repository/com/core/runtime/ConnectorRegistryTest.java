package storm.repository.com.core.runtime;

import org.junit.jupiter.api.Test;
import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.errors.ConnectorNotFoundException;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorRegistryTest {

    @Test
    void list_includesKnownConnectors() {
        ConnectorRegistry registry = new ConnectorRegistry();

        Set<String> ids = registry.list().stream()
                .map(ConnectorPlugin::id)
                .collect(Collectors.toSet());

        assertTrue(ids.contains("mongo"));
        assertTrue(ids.contains("mysql"));
    }

    @Test
    void get_unknownConnectorThrows() {
        ConnectorRegistry registry = new ConnectorRegistry();

        assertThrows(ConnectorNotFoundException.class, () -> registry.get("missing"));
    }
}
