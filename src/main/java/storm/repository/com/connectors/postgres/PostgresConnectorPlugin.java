package storm.repository.com.connectors.postgres;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class PostgresConnectorPlugin implements ConnectorPlugin {
    @Override
    public String id() {
        return "postgres";
    }

    @Override
    public String displayName() {
        return "PostgreSQL";
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
