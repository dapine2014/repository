package storm.repository.com.connectors.sqlserver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;
import storm.repository.com.core.util.SshTunnelManager;
import storm.repository.com.core.util.TunneledConnection;

import java.sql.*;
import java.util.*;

@Slf4j
@Component
public class SqlServerConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT      = 5000;
    private static final int ABSOLUTE_MAX   = 100_000;

    @Override
    public String connectorId() {
        return "sqlserver";
    }

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        String method = operation != null && operation.getMethod() != null
                ? operation.getMethod().toLowerCase() : "";
        return switch (method) {
            case "list_schema" -> listSchema(config);
            case "find"        -> find(operation, config);
            case "insert"      -> insert(operation, config);
            case "update"      -> update(operation, config);
            case "delete"      -> delete(operation, config);
            case "save"        -> save(operation, config);
            default -> throw new IllegalArgumentException("Unsupported method for sqlserver: " + method);
        };
    }

    // ── list_schema ──────────────────────────────────────────────────────────

    private List<Map<String, Object>> listSchema(Map<String, String> config) {
        log.info("Conectando a SQL Server host={} db={}", config.get("host"), config.get("database"));

        try (Connection conn = connect(config)) {
            return readSchema(conn);
        } catch (SQLException e) {
            log.error("Error conectando a SQL Server host={} db={}: {}",
                    config.get("host"), config.get("database"), e.getMessage());
            throw new RuntimeException("No se pudo conectar a SQL Server: " + sanitize(e.getMessage()), e);
        }
    }

    private List<Map<String, Object>> readSchema(Connection conn) throws SQLException {
        String sql = """
                SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
                       c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS c
                JOIN INFORMATION_SCHEMA.TABLES t
                    ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                WHERE c.TABLE_SCHEMA = 'dbo'
                  AND t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
                """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String colName   = rs.getString("COLUMN_NAME");
                String dataType  = rs.getString("DATA_TYPE");
                int charMax      = rs.getInt("CHARACTER_MAXIMUM_LENGTH");
                int numPrec      = rs.getInt("NUMERIC_PRECISION");
                int numScale     = rs.getInt("NUMERIC_SCALE");

                tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(Map.of("name", colName, "type", mapType(dataType, charMax, numPrec, numScale)));
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        tableMap.forEach((name, cols) -> result.add(Map.of("name", name, "columns", cols)));
        log.info("Schema leído SQL Server: {} tablas", result.size());
        return result;
    }

    // ── find ─────────────────────────────────────────────────────────────────

    private List<Map<String, Object>> find(RepositoryOperationDto operation, Map<String, String> config) {
        String table  = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();
        int limit = resolveEffectiveLimit(operation);

        StringBuilder sql = new StringBuilder("SELECT TOP ").append(limit)
                .append(" * FROM ").append(quote(table));
        List<Object> params = new ArrayList<>();

        if (filter != null && !filter.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(filter, sql, params);
        }

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            bindParams(stmt, params);
            return fetchRows(stmt.executeQuery());
        } catch (SQLException e) {
            throw new RuntimeException("Error en find: " + sanitize(e.getMessage()), e);
        }
    }

    // ── insert ────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> insert(RepositoryOperationDto operation, Map<String, String> config) {
        String table   = requireCollection(operation);
        Object payload = requirePayload(operation);

        if (payload instanceof List<?> list) {
            int count = 0;
            try (Connection conn = connect(config)) {
                conn.setAutoCommit(false);
                for (Object item : list) {
                    insertOne(conn, table, asMap(item, "payload item"));
                    count++;
                }
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException("Error en insert batch: " + sanitize(e.getMessage()), e);
            }
            return Map.of("insertedCount", count);
        }

        Map<String, Object> row = asMap(payload, "payload");
        try (Connection conn = connect(config)) {
            insertOne(conn, table, row);
        } catch (SQLException e) {
            throw new RuntimeException("Error en insert: " + sanitize(e.getMessage()), e);
        }
        return Map.of("insertedCount", 1);
    }

    private void insertOne(Connection conn, String table, Map<String, Object> row) throws SQLException {
        if (row.isEmpty()) throw new IllegalArgumentException("El documento a insertar está vacío");

        List<String> cols   = new ArrayList<>(row.keySet());
        List<Object> values = cols.stream().map(row::get).toList();

        String colList     = String.join(", ", cols.stream().map(this::quote).toList());
        String placeholders = String.join(", ", Collections.nCopies(cols.size(), "?"));
        String sql = "INSERT INTO " + quote(table) + " (" + colList + ") VALUES (" + placeholders + ")";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindParams(stmt, values);
            stmt.executeUpdate();
        }
    }

    // ── save (upsert via MERGE) ───────────────────────────────────────────────

    private Map<String, Object> save(RepositoryOperationDto operation, Map<String, String> config) {
        String table = requireCollection(operation);
        Map<String, Object> row = asMap(requirePayload(operation), "payload");
        if (row.isEmpty()) throw new IllegalArgumentException("El documento a guardar está vacío");

        String pkCol = row.containsKey("id") ? "id"
                     : row.containsKey("_id") ? "_id"
                     : row.keySet().iterator().next();
        Object pkVal = row.get(pkCol);

        List<String> cols   = new ArrayList<>(row.keySet());
        List<Object> values = new ArrayList<>(cols.stream().map(row::get).toList());

        // MERGE params: todas las columnas para el SELECT source, luego non-pk para UPDATE, luego todas para INSERT
        String srcCols   = String.join(", ", cols.stream()
                .map(c -> "? AS " + quote(c)).toList());
        String updateSet = String.join(", ", cols.stream()
                .filter(c -> !c.equals(pkCol))
                .map(c -> "tgt." + quote(c) + " = src." + quote(c))
                .toList());
        String insertCols = String.join(", ", cols.stream().map(this::quote).toList());
        String insertSrc  = String.join(", ", cols.stream()
                .map(c -> "src." + quote(c)).toList());

        String sql = "MERGE INTO " + quote(table) + " WITH (HOLDLOCK) AS tgt"
                + " USING (SELECT " + srcCols + ") AS src"
                + " ON tgt." + quote(pkCol) + " = src." + quote(pkCol);

        if (!updateSet.isEmpty()) {
            sql += " WHEN MATCHED THEN UPDATE SET " + updateSet;
        }
        sql += " WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES (" + insertSrc + ");";

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindParams(stmt, values);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error en save: " + sanitize(e.getMessage()), e);
        }
        return Map.of(pkCol, pkVal != null ? pkVal.toString() : "");
    }

    // ── update ────────────────────────────────────────────────────────────────

    private Map<String, Object> update(RepositoryOperationDto operation, Map<String, String> config) {
        String table = requireCollection(operation);
        Map<String, Object> filter  = operation.getFilter();
        Map<String, Object> payload = asMap(requirePayload(operation), "payload");

        if (payload.isEmpty()) throw new IllegalArgumentException("El payload de update está vacío");

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("UPDATE ").append(quote(table)).append(" SET ");

        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> e : payload.entrySet()) {
            setClauses.add(quote(e.getKey()) + " = ?");
            params.add(e.getValue());
        }
        sql.append(String.join(", ", setClauses));

        if (filter != null && !filter.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(filter, sql, params);
        }

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            bindParams(stmt, params);
            int updated = stmt.executeUpdate();
            return Map.of("modifiedCount", updated);
        } catch (SQLException e) {
            throw new RuntimeException("Error en update: " + sanitize(e.getMessage()), e);
        }
    }

    // ── delete ────────────────────────────────────────────────────────────────

    private Map<String, Object> delete(RepositoryOperationDto operation, Map<String, String> config) {
        String table = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();

        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("delete requiere al menos un filtro para evitar borrar toda la tabla");
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(quote(table)).append(" WHERE ");
        buildWhereClause(filter, sql, params);

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            bindParams(stmt, params);
            int deleted = stmt.executeUpdate();
            return Map.of("deletedCount", deleted);
        } catch (SQLException e) {
            throw new RuntimeException("Error en delete: " + sanitize(e.getMessage()), e);
        }
    }

    // ── helpers SQL ───────────────────────────────────────────────────────────

    private Connection connect(Map<String, String> config) throws SQLException {
        String host     = required(config, "host");
        String port     = config.getOrDefault("port", "1433");
        String database = required(config, "database");
        String username = required(config, "username");
        String password = required(config, "password");

        SshTunnelManager.ResolvedEndpoint endpoint =
                SshTunnelManager.resolve(config, host, Integer.parseInt(port));
        try {
            Connection raw = DriverManager.getConnection(
                    buildUrl(endpoint.host(), String.valueOf(endpoint.port()), database), username, password);
            return TunneledConnection.wrap(raw, endpoint);
        } catch (RuntimeException | SQLException e) {
            endpoint.close();
            throw e;
        }
    }

    private String buildUrl(String host, String port, String database) {
        return String.format(
                "jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false;trustServerCertificate=true",
                host, port, database
        );
    }

    private void buildWhereClause(Map<String, Object> filter, StringBuilder sql, List<Object> params) {
        List<String> conditions = new ArrayList<>();
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            if (e.getValue() == null) {
                conditions.add(quote(e.getKey()) + " IS NULL");
            } else {
                conditions.add(quote(e.getKey()) + " = ?");
                params.add(e.getValue());
            }
        }
        sql.append(String.join(" AND ", conditions));
    }

    private void bindParams(PreparedStatement stmt, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            stmt.setObject(i + 1, params.get(i));
        }
    }

    private List<Map<String, Object>> fetchRows(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    // Comilla identificadores con corchetes (SQL Server)
    private String quote(String identifier) {
        return "[" + identifier.replace("]", "]]") + "]";
    }

    private int resolveEffectiveLimit(RepositoryOperationDto op) {
        int defaultLimit = op.getDefaultLimit() != null && op.getDefaultLimit() > 0
                ? Math.min(op.getDefaultLimit(), ABSOLUTE_MAX) : DEFAULT_LIMIT;
        int maxLimit = op.getMaxLimit() != null && op.getMaxLimit() > 0
                ? Math.min(op.getMaxLimit(), ABSOLUTE_MAX) : Math.min(MAX_LIMIT, ABSOLUTE_MAX);
        int requested = op.getLimit() != null && op.getLimit() > 0 ? op.getLimit() : defaultLimit;
        return Math.min(requested, maxLimit);
    }

    // ── helpers validación ────────────────────────────────────────────────────

    private String requireCollection(RepositoryOperationDto op) {
        if (op.getCollection() == null || op.getCollection().isBlank()) {
            throw new IllegalArgumentException("Falta operation.collection");
        }
        return op.getCollection();
    }

    private Object requirePayload(RepositoryOperationDto op) {
        if (op.getPayload() == null) {
            throw new IllegalArgumentException("Falta operation.payload");
        }
        return op.getPayload();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object payload, String name) {
        if (payload instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("Se esperaba un objeto para " + name);
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

    // ── mapeo de tipos ────────────────────────────────────────────────────────

    private String mapType(String dataType, int charMax, int numPrec, int numScale) {
        if (dataType == null) return "NVARCHAR(MAX)";
        return switch (dataType.toLowerCase()) {
            case "int", "integer"                       -> "INT";
            case "bigint"                               -> "BIGINT";
            case "smallint"                             -> "SMALLINT";
            case "tinyint"                              -> "TINYINT";
            case "bit"                                  -> "BIT";
            case "float"                                -> "FLOAT";
            case "real"                                 -> "REAL";
            case "decimal", "numeric", "money",
                 "smallmoney"                           ->
                    numPrec > 0 ? String.format("DECIMAL(%d,%d)", numPrec, numScale) : "DECIMAL";
            case "nvarchar", "varchar", "sysname"       ->
                    charMax > 0 ? String.format("NVARCHAR(%d)", Math.min(charMax, 4000)) : "NVARCHAR(MAX)";
            case "nchar", "char"                        ->
                    charMax > 0 ? String.format("NCHAR(%d)", charMax) : "NCHAR(1)";
            case "ntext", "text", "xml"                 -> "NVARCHAR(MAX)";
            case "date"                                 -> "DATE";
            case "datetime", "datetime2", "smalldatetime" -> "DATETIME2";
            case "datetimeoffset"                       -> "DATETIMEOFFSET";
            case "time"                                 -> "TIME";
            case "uniqueidentifier"                     -> "UNIQUEIDENTIFIER";
            case "binary", "varbinary", "image"         -> "VARBINARY(MAX)";
            default                                     -> "NVARCHAR(MAX)";
        };
    }
}
