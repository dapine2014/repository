package storm.repository.com.connectors.mongo;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class MongoConnectorPlugin implements ConnectorPlugin {
    @Override
    public String id() {
        return "mongo";
    }

    @Override
    public String displayName() {
        return "MongoDB";
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
