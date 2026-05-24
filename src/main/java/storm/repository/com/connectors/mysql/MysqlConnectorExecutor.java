package storm.repository.com.connectors.mysql;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class MysqlConnectorExecutor implements RepositoryConnectorExecutor {

    @Override
    public String connectorId() {
        return "mysql";
    }

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        String method = operation != null && operation.getMethod() != null
                ? operation.getMethod().toLowerCase() : "";
        return switch (method) {
            case "list_schema" -> listSchema(config);
            default -> throw new IllegalArgumentException("Unsupported method for mysql: " + method);
        };
    }

    private List<Map<String, Object>> listSchema(Map<String, String> config) {
        String host     = required(config, "host");
        String port     = config.getOrDefault("port", "3306");
        String database = required(config, "database");
        String username = required(config, "username");
        String password = required(config, "password");

        String url = String.format(
                "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                host, port, database);
        log.info("Conectando a MySQL host={} db={}", host, database);

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            return readSchema(conn, database);
        } catch (SQLException e) {
            log.error("Error conectando a MySQL host={} db={}: {}", host, database, e.getMessage());
            throw new RuntimeException("No se pudo conectar a MySQL: " + sanitize(e.getMessage()), e);
        }
    }

    private List<Map<String, Object>> readSchema(Connection conn, String database) throws SQLException {
        String sql = """
                SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
                       c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE
                FROM information_schema.COLUMNS c
                JOIN information_schema.TABLES t
                    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                WHERE c.TABLE_SCHEMA = ?
                  AND t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
                """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();

        try (var stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, database);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName  = rs.getString("TABLE_NAME");
                    String colName    = rs.getString("COLUMN_NAME");
                    String dataType   = rs.getString("DATA_TYPE");
                    long   charMax    = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                    int    numPrec    = rs.getInt("NUMERIC_PRECISION");
                    int    numScale   = rs.getInt("NUMERIC_SCALE");

                    String mappedType = mapType(dataType, charMax, numPrec, numScale);

                    tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                            .add(Map.of("name", colName, "type", mappedType));
                }
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        tableMap.forEach((name, columns) ->
                result.add(Map.of("name", name, "columns", columns)));

        log.info("Schema leído: {} tablas", result.size());
        return result;
    }

    private String mapType(String dataType, long charMax, int numPrec, int numScale) {
        if (dataType == null) return "TEXT";
        return switch (dataType.toLowerCase()) {
            case "int", "integer", "int4"           -> "INTEGER";
            case "bigint", "int8"                   -> "BIGINT";
            case "smallint", "int2", "tinyint"      -> "SMALLINT";
            case "mediumint"                        -> "INTEGER";
            case "bit", "bool", "boolean"           -> "BOOLEAN";
            case "float"                            -> "REAL";
            case "double", "double precision",
                 "real"                             -> "DOUBLE PRECISION";
            case "decimal", "numeric"               ->
                    numPrec > 0 ? String.format("NUMERIC(%d,%d)", numPrec, numScale) : "NUMERIC";
            case "varchar"                          ->
                    charMax > 0 ? String.format("VARCHAR(%d)", charMax) : "TEXT";
            case "char"                             ->
                    charMax > 0 ? String.format("CHAR(%d)", charMax) : "CHAR";
            case "text", "tinytext",
                 "mediumtext", "longtext"           -> "TEXT";
            case "date"                             -> "DATE";
            case "datetime", "timestamp"            -> "TIMESTAMP";
            case "time"                             -> "TEXT";
            case "year"                             -> "SMALLINT";
            case "json"                             -> "TEXT";
            case "binary", "varbinary",
                 "blob", "tinyblob",
                 "mediumblob", "longblob"           -> "TEXT";
            default                                 -> "TEXT";
        };
    }

    private String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Falta configuración requerida: " + key);
        }
        return value;
    }

    private String sanitize(String msg) {
        if (msg == null) return "";
        return msg.replaceAll("://[^\\s:@/]+:[^\\s@/]+@", "://***:***@");
    }
}
