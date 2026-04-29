package storm.repository.com.connectors.mysql;

import storm.repository.com.core.api.ConnectorConfig;

public final class MysqlConnectorConfig {

    public static final String HOST     = "host";
    public static final String PORT     = "port";
    public static final String DATABASE = "database";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String URL      = "url";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String database;

    public MysqlConnectorConfig(ConnectorConfig config) {
        this.username = config.getRequired(USERNAME);
        this.password = config.getRequired(PASSWORD);
        this.database = config.getRequired(DATABASE);

        String url = config.get(URL);
        if (url != null && !url.isBlank()) {
            this.jdbcUrl = url;
        } else {
            String host = config.getRequired(HOST);
            String port = config.get(PORT) != null ? config.get(PORT) : "3306";
            this.jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                host, port, database);
        }
    }

    public String jdbcUrl()  { return jdbcUrl; }
    public String username() { return username; }
    public String password() { return password; }
    public String database() { return database; }
}
