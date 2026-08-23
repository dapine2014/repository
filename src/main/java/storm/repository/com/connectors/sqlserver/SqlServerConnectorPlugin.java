package storm.repository.com.connectors.sqlserver;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class SqlServerConnectorPlugin implements ConnectorPlugin {
    @Override
    public String id() {
        return "sqlserver";
    }

    @Override
    public String displayName() {
        return "SQL Server";
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
