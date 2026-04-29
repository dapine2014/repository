package storm.repository.com.connectors.postgresql;

import org.springframework.stereotype.Component;
import storm.repository.com.core.api.ConnectorConfig;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.sql.*;
import java.util.*;

@Component
public class PostgreSQLConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT  = 200;
    private static final int MAX_LIMIT      = 5000;

    @Override
    public String connectorId() {
        return "postgres";
    }

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        PostgreSQLConnectorConfig cfg = new PostgreSQLConnectorConfig(new ConnectorConfig(config));
        validateOperation(operation);

        try (Connection conn = DriverManager.getConnection(cfg.jdbcUrl(), cfg.username(), cfg.password())) {
            return switch (operation.getMethod().toLowerCase()) {
                case "list_schema" -> listSchema(conn);
                case "find"        -> find(conn, operation);
                case "insert"      -> insert(conn, operation, cfg.database());
                case "update"      -> update(conn, operation);
                case "delete"      -> delete(conn, operation);
                default -> throw new IllegalArgumentException("Unsupported method: " + operation.getMethod());
            };
        } catch (SQLException e) {
            throw new RuntimeException("PostgreSQL error: " + e.getMessage(), e);
        }
    }

    // ── list_schema ──────────────────────────────────────────────────────────

    private List<Map<String, Object>> listSchema(Connection conn) throws SQLException {
        String sql = """
            SELECT t.table_name, c.column_name, c.data_type, c.is_nullable
            FROM information_schema.tables t
            JOIN information_schema.columns c
              ON t.table_name = c.table_name AND t.table_schema = c.table_schema
            WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position
            """;

        Map<String, List<Map<String, Object>>> tableMap = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                tableMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(Map.of(
                                "name",     rs.getString("column_name"),
                                "type",     rs.getString("data_type"),
                                "nullable", rs.getString("is_nullable")
                        ));
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

        StringBuilder sql = new StringBuilder("SELECT * FROM \"").append(table).append("\"");
        List<Object> params = new ArrayList<>();

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> {
                where.add("\"" + k + "\" = ?");
                params.add(v);
            });
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

        ensureTableExists(conn, table, records.get(0));

        Set<String> columns = records.get(0).keySet();
        String colList  = columns.stream().map(c -> "\"" + c + "\"").reduce((a, b) -> a + "," + b).orElse("");
        String valList  = columns.stream().map(c -> "?").reduce((a, b) -> a + "," + b).orElse("");
        String sql      = "INSERT INTO \"" + table + "\" (" + colList + ") VALUES (" + valList + ")";

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

    private void ensureTableExists(Connection conn, String table, Map<String, Object> sample) throws SQLException {
        String check = "SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=?";
        try (PreparedStatement ps = conn.prepareStatement(check)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return;
            }
        }

        StringBuilder ddl = new StringBuilder("CREATE TABLE \"").append(table).append("\" (");
        StringJoiner cols = new StringJoiner(", ");
        sample.forEach((col, val) -> cols.add("\"" + col + "\" " + inferType(val)));
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

        StringBuilder sql = new StringBuilder("UPDATE \"").append(table).append("\" SET ");
        List<Object> params = new ArrayList<>();

        StringJoiner sets = new StringJoiner(", ");
        payload.forEach((k, v) -> { sets.add("\"" + k + "\" = ?"); params.add(v); });
        sql.append(sets);

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> { where.add("\"" + k + "\" = ?"); params.add(v); });
            sql.append(where);
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            int updated = ps.executeUpdate();
            return Map.of("updatedCount", updated);
        }
    }

    // ── delete ───────────────────────────────────────────────────────────────

    private Map<String, Object> delete(Connection conn, RepositoryOperationDto op) throws SQLException {
        String table = op.getCollection();
        StringBuilder sql = new StringBuilder("DELETE FROM \"").append(table).append("\"");
        List<Object> params = new ArrayList<>();

        if (op.getFilter() != null && !op.getFilter().isEmpty()) {
            sql.append(" WHERE ");
            StringJoiner where = new StringJoiner(" AND ");
            op.getFilter().forEach((k, v) -> { where.add("\"" + k + "\" = ?"); params.add(v); });
            sql.append(where);
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            int deleted = ps.executeUpdate();
            return Map.of("deletedCount", deleted);
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
        if (value instanceof Integer)  return "INTEGER";
        if (value instanceof Long)     return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE PRECISION";
        return "TEXT";
    }
}
