package storm.repository.com.connectors.mongo;

import storm.repository.com.core.api.ConnectorConfig;

public final class MongoConnectorConfig {
    public static final String URI = "uri";
    public static final String DATABASE = "database";

    private final String uri;
    private final String database;

    public MongoConnectorConfig(ConnectorConfig config) {
        this.uri = config.getRequired(URI);
        this.database = config.getRequired(DATABASE);
    }

    public String uri() {
        return uri;
    }

    public String database() {
        return database;
    }
}
