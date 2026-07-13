package storm.repository.com.connectors.oracle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.sql.*;
import java.util.*;

@Slf4j
@Component
public class OracleConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT      = 5000;
    private static final int ABSOLUTE_MAX   = 100_000;

    @Override
    public String connectorId() {
        return "oracle";
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
            default -> throw new IllegalArgumentException("Unsupported method for oracle: " + method);
        };
    }

    // ── list_schema ───────────────────────────────────────────────────────────

    private List<Map<String, Object>> listSchema(Map<String, String> config) {
        String host     = required(config, "host");
        String database = required(config, "database");
        log.info("Conectando a Oracle host={} service={}", host, database);

        try (Connection conn = connect(config)) {
            return readSchema(conn);
        } catch (SQLException e) {
            log.error("Error conectando a Oracle host={} service={}: {}", host, database, e.getMessage());
            throw new RuntimeException("No se pudo conectar a Oracle: " + sanitize(e.getMessage()), e);
        }
    }

    private List<Map<String, Object>> readSchema(Connection conn) throws SQLException {
        // USER_TAB_COLUMNS muestra solo las tablas del usuario conectado
        String sql = """
                SELECT t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
                       c.CHAR_LENGTH, c.DATA_PRECISION, c.DATA_SCALE
                FROM USER_TABLES t
                JOIN USER_TAB_COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
                ORDER BY c.TABLE_NAME, c.COLUMN_ID
                """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String colName   = rs.getString("COLUMN_NAME");
                String dataType  = rs.getString("DATA_TYPE");
                int    charLen   = rs.getInt("CHAR_LENGTH");
                int    precision = rs.getInt("DATA_PRECISION");
                int    scale     = rs.getInt("DATA_SCALE");

                tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(Map.of("name", colName, "type", mapType(dataType, charLen, precision, scale)));
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        tableMap.forEach((name, cols) -> result.add(Map.of("name", name, "columns", cols)));
        log.info("Schema leído: {} tablas", result.size());
        return result;
    }

    // ── find ──────────────────────────────────────────────────────────────────

    private List<Map<String, Object>> find(RepositoryOperationDto operation, Map<String, String> config) {
        String table = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();
        int limit = resolveEffectiveLimit(operation);

        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(quote(table));
        List<Object> params = new ArrayList<>();

        if (filter != null && !filter.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(filter, sql, params);
        }
        // Oracle 12c+ soporta FETCH FIRST
        sql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            bindParams(stmt, params);
            return fetchRows(stmt.executeQuery());
        } catch (SQLException e) {
            throw new RuntimeException("Error en find: " + sanitize(e.getMessage()), e);
        }
    }

    // ── insert ────────────────────────────────────────────────────────────────

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

        try (Connection conn = connect(config)) {
            insertOne(conn, table, asMap(payload, "payload"));
        } catch (SQLException e) {
            throw new RuntimeException("Error en insert: " + sanitize(e.getMessage()), e);
        }
        return Map.of("insertedCount", 1);
    }

    private void insertOne(Connection conn, String table, Map<String, Object> row) throws SQLException {
        if (row.isEmpty()) throw new IllegalArgumentException("El documento a insertar está vacío");

        List<String> cols   = new ArrayList<>(row.keySet());
        List<Object> values = cols.stream().map(row::get).toList();

        String colList      = String.join(", ", cols.stream().map(this::quote).toList());
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

        List<String> cols    = new ArrayList<>(row.keySet());
        List<String> nonPk   = cols.stream().filter(c -> !c.equals(pkCol)).toList();

        // MERGE INTO tabla USING DUAL ON (pk = ?) WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...
        String updateSet = String.join(", ", nonPk.stream()
                .map(c -> "t." + quote(c) + " = s." + quote(c)).toList());
        String insertCols = String.join(", ", cols.stream().map(this::quote).toList());
        String insertVals = String.join(", ", cols.stream().map(c -> "s." + quote(c)).toList());
        String usingCols  = String.join(", ", cols.stream()
                .map(c -> "? AS " + quote(c)).toList());

        String sql = "MERGE INTO " + quote(table) + " t "
                + "USING (SELECT " + usingCols + " FROM DUAL) s "
                + "ON (t." + quote(pkCol) + " = s." + quote(pkCol) + ") "
                + "WHEN MATCHED THEN UPDATE SET " + updateSet + " "
                + "WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES (" + insertVals + ")";

        List<Object> params = new ArrayList<>(cols.stream().map(row::get).toList());

        try (Connection conn = connect(config);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindParams(stmt, params);
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
        String port     = config.getOrDefault("port", "1521");
        String database = required(config, "database");
        String username = required(config, "username");
        String password = required(config, "password");
        // Soporta tanto SID (jdbc:oracle:thin:@host:port:SID)
        // como Service Name (jdbc:oracle:thin:@//host:port/service)
        String url = database.contains("/")
                ? String.format("jdbc:oracle:thin:@//%s:%s/%s", host, port, database)
                : String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, database);
        return DriverManager.getConnection(url, username, password);
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

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
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
        if (op.getCollection() == null || op.getCollection().isBlank())
            throw new IllegalArgumentException("Falta operation.collection");
        return op.getCollection();
    }

    private Object requirePayload(RepositoryOperationDto op) {
        if (op.getPayload() == null)
            throw new IllegalArgumentException("Falta operation.payload");
        return op.getPayload();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object payload, String name) {
        if (payload instanceof Map<?, ?> map) return (Map<String, Object>) map;
        throw new IllegalArgumentException("Se esperaba un objeto para " + name);
    }

    private String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Falta configuración requerida: " + key);
        return value;
    }

    private String sanitize(String msg) {
        if (msg == null) return "";
        return msg.replaceAll("://[^\\s:@/]+:[^\\s@/]+@", "://***:***@");
    }

    // ── mapeo de tipos ────────────────────────────────────────────────────────

    private String mapType(String dataType, int charLen, int precision, int scale) {
        if (dataType == null) return "TEXT";
        return switch (dataType.toUpperCase()) {
            case "NUMBER"           -> precision > 0
                                        ? (scale == 0 ? "BIGINT" : String.format("NUMERIC(%d,%d)", precision, scale))
                                        : "NUMERIC";
            case "INTEGER", "INT"   -> "INTEGER";
            case "FLOAT"            -> "DOUBLE PRECISION";
            case "BINARY_FLOAT"     -> "REAL";
            case "BINARY_DOUBLE"    -> "DOUBLE PRECISION";
            case "VARCHAR2",
                 "NVARCHAR2"        -> charLen > 0 ? String.format("VARCHAR(%d)", charLen) : "TEXT";
            case "CHAR", "NCHAR"    -> charLen > 0 ? String.format("CHAR(%d)", charLen) : "CHAR";
            case "CLOB", "NCLOB",
                 "LONG"             -> "TEXT";
            case "DATE"             -> "TIMESTAMP";
            case "TIMESTAMP",
                 "TIMESTAMP WITH TIME ZONE",
                 "TIMESTAMP WITH LOCAL TIME ZONE" -> "TIMESTAMP";
            case "BLOB", "RAW",
                 "LONG RAW"         -> "TEXT";
            default                 -> "TEXT";
        };
    }
}
