package storm.repository.com.connectors.redis;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class RedisConnectorPlugin implements ConnectorPlugin {
    @Override public String id()          { return "redis"; }
    @Override public String displayName() { return "Redis"; }
    @Override public String version()     { return "1.0.0"; }
    @Override public ConnectorType type() { return ConnectorType.SOURCE; }
}
