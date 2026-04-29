package storm.repository.com.connectors.mysql;

import org.springframework.stereotype.Component;
import storm.repository.com.core.api.ConnectorConfig;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.sql.*;
import java.util.*;

@Component
public class MysqlConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT     = 5000;

    @Override
    public String connectorId() {
        return "mysql";
    }

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        MysqlConnectorConfig cfg = new MysqlConnectorConfig(new ConnectorConfig(config));
        validateOperation(operation);

        try (Connection conn = DriverManager.getConnection(cfg.jdbcUrl(), cfg.username(), cfg.password())) {
            return switch (operation.getMethod().toLowerCase()) {
                case "list_schema" -> listSchema(conn, cfg.database());
                case "find"        -> find(conn, operation);
                case "insert"      -> insert(conn, operation, cfg.database());
                case "update"      -> update(conn, operation);
                case "delete"      -> delete(conn, operation);
                default -> throw new IllegalArgumentException("Unsupported method: " + operation.getMethod());
            };
        } catch (SQLException e) {
            throw new RuntimeException("MySQL error: " + e.getMessage(), e);
        }
    }

    // ── list_schema ──────────────────────────────────────────────────────────

    private List<Map<String, Object>> listSchema(Connection conn, String database) throws SQLException {
        String sql = """
            SELECT t.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE
            FROM information_schema.TABLES t
            JOIN information_schema.COLUMNS c
              ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
            """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                            .add(Map.of(
                                    "name",     rs.getString("COLUMN_NAME"),
                                    "type",     rs.getString("DATA_TYPE"),
                                    "nullable", rs.getString("IS_NULLABLE")
                            ));
                }
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        tableMap.forEach((name, columns) ->
                result.add(Map.of("name", name, "columns", columns)));
        return result;
    }

    // ── find ─────────────────────────────────────────────────────────────────

    private List<Map<String, Object>> find(Connection conn, RepositoryOperationDto op) throws SQLException {
        String table  = op.getCollection();
        int    limit  = resolveLimit(op.getLimit());
        int    offset = resolveOffset(op.getCursor());

        StringBuilder sql = new StringBuilder("SELECT * FROM `").append(table).append("`");
        List<Object> params = new ArrayList<>();

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> { where.add("`" + k + "` = ?"); params.add(v); });
            sql.append(where);
        }

        sql.append(" LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        List<Map<String, Object>> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= cols; i++) row.put(meta.getColumnName(i), rs.getObject(i));
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    // ── insert ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> insert(Connection conn, RepositoryOperationDto op, String database) throws SQLException {
        String table   = op.getCollection();
        Object payload = op.getPayload();
        if (payload == null) throw new IllegalArgumentException("Missing payload for insert");

        List<Map<String, Object>> records = payload instanceof List<?>
                ? (List<Map<String, Object>>) payload
                : List.of((Map<String, Object>) payload);

        if (records.isEmpty()) return Map.of("insertedCount", 0);

        ensureTableExists(conn, table, database, records.get(0));

        Set<String> columns = records.get(0).keySet();
        String colList = columns.stream().map(c -> "`" + c + "`").reduce((a, b) -> a + "," + b).orElse("");
        String valList = columns.stream().map(c -> "?").reduce((a, b) -> a + "," + b).orElse("");
        String sql     = "INSERT INTO `" + table + "` (" + colList + ") VALUES (" + valList + ")";

        int inserted = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : records) {
                int idx = 1;
                for (String col : columns) ps.setObject(idx++, row.get(col));
                ps.addBatch();
                inserted++;
            }
            ps.executeBatch();
        }
        return Map.of("insertedCount", inserted);
    }

    private void ensureTableExists(Connection conn, String table, String database, Map<String, Object> sample) throws SQLException {
        String check = "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        try (PreparedStatement ps = conn.prepareStatement(check)) {
            ps.setString(1, database);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return;
            }
        }

        StringBuilder ddl = new StringBuilder("CREATE TABLE `").append(table).append("` (");
        StringJoiner cols = new StringJoiner(", ");
        sample.forEach((col, val) -> cols.add("`" + col + "` " + inferType(val)));
        ddl.append(cols).append(")");

        try (Statement st = conn.createStatement()) {
            st.execute(ddl.toString());
        }
    }

    // ── update ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> update(Connection conn, RepositoryOperationDto op) throws SQLException {
        String table   = op.getCollection();
        Map<String, Object> payload = (Map<String, Object>) op.getPayload();
        if (payload == null || payload.isEmpty()) throw new IllegalArgumentException("Missing payload for update");

        StringBuilder sql = new StringBuilder("UPDATE `").append(table).append("` SET ");
        List<Object> params = new ArrayList<>();

        StringJoiner sets = new StringJoiner(", ");
        payload.forEach((k, v) -> { sets.add("`" + k + "` = ?"); params.add(v); });
        sql.append(sets);

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> { where.add("`" + k + "` = ?"); params.add(v); });
            sql.append(where);
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            return Map.of("updatedCount", ps.executeUpdate());
        }
    }

    // ── delete ───────────────────────────────────────────────────────────────

    private Map<String, Object> delete(Connection conn, RepositoryOperationDto op) throws SQLException {
        String table = op.getCollection();
        StringBuilder sql = new StringBuilder("DELETE FROM `").append(table).append("`");
        List<Object> params = new ArrayList<>();

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> { where.add("`" + k + "` = ?"); params.add(v); });
            sql.append(where);
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            return Map.of("deletedCount", ps.executeUpdate());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void validateOperation(RepositoryOperationDto op) {
        if (op == null) throw new IllegalArgumentException("Missing operation");
        if (op.getMethod() == null || op.getMethod().isBlank()) throw new IllegalArgumentException("Missing operation.method");
        if (!"list_schema".equals(op.getMethod().toLowerCase()) &&
                (op.getCollection() == null || op.getCollection().isBlank()))
            throw new IllegalArgumentException("Missing operation.collection");
    }

    private int resolveLimit(Integer requested) {
        if (requested != null && requested > 0) return Math.min(requested, MAX_LIMIT);
        return DEFAULT_LIMIT;
    }

    private int resolveOffset(String cursor) {
        if (cursor == null || cursor.isBlank()) return 0;
        try { return Integer.parseInt(cursor); } catch (NumberFormatException e) { return 0; }
    }

    private String inferType(Object value) {
        if (value instanceof Boolean)  return "BOOLEAN";
        if (value instanceof Integer)  return "INT";
        if (value instanceof Long)     return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE";
        return "TEXT";
    }
}
