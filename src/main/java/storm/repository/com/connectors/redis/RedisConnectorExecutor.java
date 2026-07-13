package storm.repository.com.connectors.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.util.*;

/**
 * Connector Redis para storm-repository.
 *
 * Modelo de datos: cada "documento" se almacena como un Redis Hash bajo la clave
 * "{collection}:{id}". El campo "id" o "_id" dentro del payload actúa como
 * identificador; si no existe se genera un UUID.
 *
 * Config keys: host, port (default 6379), password (opcional), database (índice 0-15, default 0)
 */
@Slf4j
@Component
public class RedisConnectorExecutor implements RepositoryConnectorExecutor {

    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT      = 5000;
    private static final int ABSOLUTE_MAX   = 100_000;

    private static final String CURSOR_START = "0";

    @Override
    public String connectorId() {
        return "redis";
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
            default -> throw new IllegalArgumentException("Unsupported method for redis: " + method);
        };
    }

    // ── list_schema ───────────────────────────────────────────────────────────
    // Descubre prefijos de claves existentes y muestra los campos del primer hash encontrado.

    private List<Map<String, Object>> listSchema(Map<String, String> config) {
        log.info("Conectando a Redis host={}", config.get("host"));
        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            Map<String, List<Map<String, Object>>> prefixMap = new LinkedHashMap<>();
            ScanParams params = new ScanParams().match("*:*").count(500);
            String cursor = CURSOR_START;
            int scanned = 0;

            do {
                ScanResult<String> result = jedis.scan(cursor, params);
                cursor = result.getCursor();
                for (String key : result.getResult()) {
                    String prefix = key.contains(":") ? key.substring(0, key.lastIndexOf(':')) : key;
                    if (!prefixMap.containsKey(prefix) && jedis.type(key).equals("hash")) {
                        Map<String, String> sample = jedis.hgetAll(key);
                        List<Map<String, Object>> cols = sample.keySet().stream()
                                .map(f -> Map.<String, Object>of("name", f, "type", "TEXT"))
                                .toList();
                        prefixMap.put(prefix, cols);
                    }
                    if (++scanned >= 2000) break;
                }
            } while (!cursor.equals(CURSOR_START) && scanned < 2000);

            List<Map<String, Object>> tables = new ArrayList<>();
            prefixMap.forEach((name, cols) -> tables.add(Map.of("name", name, "columns", cols)));
            log.info("Schema leído: {} prefijos", tables.size());
            return tables;
        }
    }

    // ── find ──────────────────────────────────────────────────────────────────

    private List<Map<String, Object>> find(RepositoryOperationDto operation, Map<String, String> config) {
        String collection = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();
        int limit = resolveEffectiveLimit(operation);

        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            List<Map<String, Object>> results = new ArrayList<>();
            ScanParams params = new ScanParams().match(collection + ":*").count(200);
            String cursor = CURSOR_START;

            do {
                ScanResult<String> scan = jedis.scan(cursor, params);
                cursor = scan.getCursor();
                for (String key : scan.getResult()) {
                    if (!"hash".equals(jedis.type(key))) continue;
                    Map<String, String> hash = jedis.hgetAll(key);
                    Map<String, Object> doc = new LinkedHashMap<>(hash);
                    doc.put("id", key.substring(collection.length() + 1));
                    if (matchesFilter(doc, filter)) {
                        results.add(doc);
                        if (results.size() >= limit) return results;
                    }
                }
            } while (!cursor.equals(CURSOR_START) && results.size() < limit);

            return results;
        }
    }

    // ── insert ────────────────────────────────────────────────────────────────

    private Map<String, Object> insert(RepositoryOperationDto operation, Map<String, String> config) {
        String collection = requireCollection(operation);
        Object payload    = requirePayload(operation);

        if (payload instanceof List<?> list) {
            try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
                for (Object item : list) {
                    Map<String, Object> doc = asMap(item, "payload item");
                    String id = resolveId(doc);
                    jedis.hset(collection + ":" + id, toStringMap(doc));
                }
                return Map.of("insertedCount", list.size());
            }
        }

        Map<String, Object> doc = asMap(payload, "payload");
        String id = resolveId(doc);
        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            jedis.hset(collection + ":" + id, toStringMap(doc));
        }
        return Map.of("insertedCount", 1, "id", id);
    }

    // ── save (upsert) ─────────────────────────────────────────────────────────

    private Map<String, Object> save(RepositoryOperationDto operation, Map<String, String> config) {
        String collection = requireCollection(operation);
        Map<String, Object> doc = asMap(requirePayload(operation), "payload");
        String id = resolveId(doc);

        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            jedis.hset(collection + ":" + id, toStringMap(doc));
        }
        return Map.of("id", id);
    }

    // ── update ────────────────────────────────────────────────────────────────

    private Map<String, Object> update(RepositoryOperationDto operation, Map<String, String> config) {
        String collection = requireCollection(operation);
        Map<String, Object> filter  = operation.getFilter();
        Map<String, Object> payload = asMap(requirePayload(operation), "payload");
        if (payload.isEmpty()) throw new IllegalArgumentException("El payload de update está vacío");

        int modified = 0;
        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            List<Map<String, Object>> docs = find(operation, config);
            for (Map<String, Object> doc : docs) {
                String id = doc.get("id") != null ? doc.get("id").toString() : null;
                if (id == null) continue;
                jedis.hset(collection + ":" + id, toStringMap(payload));
                modified++;
            }
        }
        return Map.of("modifiedCount", modified);
    }

    // ── delete ────────────────────────────────────────────────────────────────

    private Map<String, Object> delete(RepositoryOperationDto operation, Map<String, String> config) {
        String collection = requireCollection(operation);
        Map<String, Object> filter = operation.getFilter();
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("delete requiere al menos un filtro para evitar borrar toda la colección");
        }

        // Si el filtro tiene exactamente "id", borrado directo sin SCAN
        if (filter.size() == 1 && filter.containsKey("id")) {
            String key = collection + ":" + filter.get("id");
            try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
                long deleted = jedis.del(key);
                return Map.of("deletedCount", deleted);
            }
        }

        List<Map<String, Object>> docs = find(operation, config);
        if (docs.isEmpty()) return Map.of("deletedCount", 0);

        try (JedisPool pool = buildPool(config); Jedis jedis = pool.getResource()) {
            String[] keys = docs.stream()
                    .filter(d -> d.get("id") != null)
                    .map(d -> collection + ":" + d.get("id"))
                    .toArray(String[]::new);
            long deleted = keys.length > 0 ? jedis.del(keys) : 0;
            return Map.of("deletedCount", deleted);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private JedisPool buildPool(Map<String, String> config) {
        String host     = required(config, "host");
        int    port     = Integer.parseInt(config.getOrDefault("port", "6379"));
        String password = config.get("password");
        int    dbIndex  = Integer.parseInt(config.getOrDefault("database", "0"));

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(4);
        poolConfig.setMaxIdle(2);

        return (password != null && !password.isBlank())
                ? new JedisPool(poolConfig, host, port, 3000, password, dbIndex)
                : new JedisPool(poolConfig, host, port, 3000, null, dbIndex);
    }

    private boolean matchesFilter(Map<String, Object> doc, Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) return true;
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            Object docVal = doc.get(e.getKey());
            if (e.getValue() == null) {
                if (docVal != null) return false;
            } else {
                if (docVal == null || !e.getValue().toString().equals(docVal.toString())) return false;
            }
        }
        return true;
    }

    private String resolveId(Map<String, Object> doc) {
        Object id = doc.containsKey("id") ? doc.get("id") : doc.get("_id");
        return (id != null && !id.toString().isBlank()) ? id.toString() : UUID.randomUUID().toString();
    }

    private Map<String, String> toStringMap(Map<String, Object> doc) {
        Map<String, String> result = new LinkedHashMap<>();
        doc.forEach((k, v) -> {
            if (v != null) result.put(k, v.toString());
        });
        return result;
    }

    private int resolveEffectiveLimit(RepositoryOperationDto op) {
        int defaultLimit = op.getDefaultLimit() != null && op.getDefaultLimit() > 0
                ? Math.min(op.getDefaultLimit(), ABSOLUTE_MAX) : DEFAULT_LIMIT;
        int maxLimit = op.getMaxLimit() != null && op.getMaxLimit() > 0
                ? Math.min(op.getMaxLimit(), ABSOLUTE_MAX) : Math.min(MAX_LIMIT, ABSOLUTE_MAX);
        int requested = op.getLimit() != null && op.getLimit() > 0 ? op.getLimit() : defaultLimit;
        return Math.min(requested, maxLimit);
    }

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
}
