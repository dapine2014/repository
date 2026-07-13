package storm.repository.com.connectors.db2;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class Db2ConnectorPlugin implements ConnectorPlugin {
    @Override
    public String id() {
        return "db2";
    }

    @Override
    public String displayName() {
        return "IBM DB2";
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
