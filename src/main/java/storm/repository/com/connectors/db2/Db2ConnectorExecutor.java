package storm.repository.com.connectors.db2;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.sql.*;
import java.util.*;

@Slf4j
@Component
public class Db2ConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT      = 5000;
    private static final int ABSOLUTE_MAX   = 100_000;

    @Override
    public String connectorId() {
        return "db2";
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
            default -> throw new IllegalArgumentException("Unsupported method for db2: " + method);
        };
    }

    // ── list_schema ──────────────────────────────────────────────────────────

    private List<Map<String, Object>> listSchema(Map<String, String> config) {
        String host     = required(config, "host");
        String port     = config.getOrDefault("port", "50000");
        String database = required(config, "database");
        String username = required(config, "username");
        String password = required(config, "password");

        log.info("Conectando a DB2 host={} db={}", host, database);

        try (Connection conn = DriverManager.getConnection(buildUrl(host, port, database), username, password)) {
            return readSchema(conn, username);
        } catch (SQLException e) {
            log.error("Error conectando a DB2 host={} db={}: {}", host, database, e.getMessage());
            throw new RuntimeException("No se pudo conectar a DB2: " + sanitize(e.getMessage()), e);
        }
    }

    private List<Map<String, Object>> readSchema(Connection conn, String username) throws SQLException {
        // SYSCAT.COLUMNS — filtrar por el schema del usuario (uppercased, como DB2 almacena)
        String schema = username.toUpperCase();
        String sql = """
                SELECT c.TABNAME, c.COLNAME, c.TYPENAME, c.LENGTH, c.SCALE
                FROM SYSCAT.COLUMNS c
                JOIN SYSCAT.TABLES t ON c.TABSCHEMA = t.TABSCHEMA AND c.TABNAME = t.TABNAME
                WHERE c.TABSCHEMA = ?
                  AND t.TYPE = 'T'
                ORDER BY c.TABNAME, c.COLNO
                """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABNAME");
                    String colName   = rs.getString("COLNAME").toLowerCase();
                    String typeName  = rs.getString("TYPENAME");
                    int length       = rs.getInt("LENGTH");
                    int scale        = rs.getInt("SCALE");

                    tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                            .add(Map.of("name", colName, "type", mapType(typeName, length, scale)));
                }
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        tableMap.forEach((name, cols) -> result.add(Map.of("name", name.toLowerCase(), "columns", cols)));
        log.info("Schema leído DB2: {} tablas", result.size());
        return result;
    }

    // ── find ─────────────────────────────────────────────────────────────────

    private List<Map<String, Object>> find(RepositoryOperationDto operation, Map<String, String> config) {
        String table  = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();
        int limit = resolveEffectiveLimit(operation);

        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(quote(table));
        List<Object> params = new ArrayList<>();

        if (filter != null && !filter.isEmpty()) {
            sql.append(" WHERE ");
            buildWhereClause(filter, sql, params);
        }
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

        String srcCols    = String.join(", ", cols.stream()
                .map(c -> "? AS " + quote(c)).toList());
        String updateSet  = String.join(", ", cols.stream()
                .filter(c -> !c.equals(pkCol))
                .map(c -> "tgt." + quote(c) + " = src." + quote(c))
                .toList());
        String insertCols = String.join(", ", cols.stream().map(this::quote).toList());
        String insertSrc  = String.join(", ", cols.stream()
                .map(c -> "src." + quote(c)).toList());

        String sql = "MERGE INTO " + quote(table) + " AS tgt"
                + " USING (SELECT " + srcCols + " FROM SYSIBM.SYSDUMMY1) AS src"
                + " ON tgt." + quote(pkCol) + " = src." + quote(pkCol);

        if (!updateSet.isEmpty()) {
            sql += " WHEN MATCHED THEN UPDATE SET " + updateSet;
        }
        sql += " WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES (" + insertSrc + ")";

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
        String port     = config.getOrDefault("port", "50000");
        String database = required(config, "database");
        String username = required(config, "username");
        String password = required(config, "password");
        return DriverManager.getConnection(buildUrl(host, port, database), username, password);
    }

    private String buildUrl(String host, String port, String database) {
        return String.format("jdbc:db2://%s:%s/%s", host, port, database);
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
                // DB2 devuelve nombres de columna en mayúsculas — los normalizamos a minúsculas
                row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    // Comilla identificadores con dobles comillas en mayúsculas (DB2)
    private String quote(String identifier) {
        return "\"" + identifier.toUpperCase().replace("\"", "\"\"") + "\"";
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

    private String mapType(String dataType, int length, int scale) {
        if (dataType == null) return "VARCHAR(255)";
        return switch (dataType.toUpperCase().trim()) {
            case "INTEGER", "INT"                   -> "INTEGER";
            case "BIGINT"                           -> "BIGINT";
            case "SMALLINT"                         -> "SMALLINT";
            case "DOUBLE", "DOUBLE PRECISION",
                 "FLOAT"                            -> "DOUBLE";
            case "REAL"                             -> "REAL";
            case "DECIMAL", "NUMERIC"               ->
                    length > 0 ? String.format("DECIMAL(%d,%d)", length, scale) : "DECIMAL";
            case "VARCHAR", "CHARACTER VARYING"     ->
                    length > 0 ? String.format("VARCHAR(%d)", length) : "VARCHAR(255)";
            case "CHAR", "CHARACTER"                ->
                    length > 0 ? String.format("CHAR(%d)", length) : "CHAR(1)";
            case "CLOB", "DBCLOB", "LONG VARCHAR"   -> "CLOB(1M)";
            case "DATE"                             -> "DATE";
            case "TIMESTAMP", "TIMESTAMPTZ"         -> "TIMESTAMP";
            case "TIME"                             -> "TIME";
            case "BOOLEAN", "BOOL"                  -> "BOOLEAN";
            default                                 -> "VARCHAR(255)";
        };
    }
}
