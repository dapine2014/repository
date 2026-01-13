package storm.repository.com.core.api;

public interface ConnectorPlugin {
    String id();

    String displayName();

    String version();

    ConnectorType type();
}