package storm.repository.com.core.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class ConnectorConfig {
    private final Map<String, String> properties;

    public ConnectorConfig(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(Objects.requireNonNull(properties, "properties"));
    }

    public String get(String key) {
        return properties.get(key);
    }

    public String getRequired(String key) {
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return value;
    }

    public Map<String, String> asMap() {
        return properties;
    }
}
