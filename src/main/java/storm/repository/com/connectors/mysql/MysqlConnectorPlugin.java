package storm.repository.com.connectors.mysql;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class MysqlConnectorPlugin implements ConnectorPlugin {
    @Override
    public String id() {
        return "mysql";
    }

    @Override
    public String displayName() {
        return "MySQL";
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public ConnectorType type() {
        return ConnectorType.SOURCE;
    }
}
