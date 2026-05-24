package storm.repository.com.connectors.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;
import storm.repository.com.core.api.ConnectorConfig;
import storm.repository.com.core.dto.RepositoryOperationDto;
import storm.repository.com.core.runtime.RepositoryConnectorExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
public class MongoConnectorExecutor implements RepositoryConnectorExecutor {
    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 5000;
    private static final int ABSOLUTE_MAX_LIMIT = 100000;
    @Override
    public String connectorId() {
        return "mongo";
    }

    private static final Set<String> NO_COLLECTION_METHODS = Set.of("list_schema", "drop_database");

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        MongoConnectorConfig mongoConfig = new MongoConnectorConfig(new ConnectorConfig(config));
        validateOperation(operation);

        try (MongoClient client = MongoConnectorClient.createClient(mongoConfig)) {
            MongoDatabase database = client.getDatabase(mongoConfig.database());
            String method = operation.getMethod().toLowerCase();

            return switch (method) {
                case "list_schema"     -> listSchema(database);
                case "drop_database"   -> dropDatabase(database, mongoConfig.database());
                case "drop_collection" -> dropCollection(database, operation.getCollection());
                case "save"            -> save(database.getCollection(operation.getCollection()), operation);
                case "find"            -> find(database.getCollection(operation.getCollection()), operation);
                case "insert"          -> insert(database.getCollection(operation.getCollection()), operation);
                case "update"          -> update(database.getCollection(operation.getCollection()), operation);
                case "delete"          -> delete(database.getCollection(operation.getCollection()), operation);
                default -> throw new IllegalArgumentException("Unsupported method: " + operation.getMethod());
            };
        }
    }

    private void validateOperation(RepositoryOperationDto operation) {
        if (operation == null) {
            throw new IllegalArgumentException("Missing operation");
        }
        if (operation.getMethod() == null || operation.getMethod().isBlank()) {
            throw new IllegalArgumentException("Missing operation.method");
        }
        String method = operation.getMethod().toLowerCase();
        if (!NO_COLLECTION_METHODS.contains(method) &&
                (operation.getCollection() == null || operation.getCollection().isBlank())) {
            throw new IllegalArgumentException("Missing operation.collection");
        }
    }

    private Object dropDatabase(MongoDatabase database, String dbName) {
        database.drop();
        return Map.of("dropped", dbName);
    }

    private Object dropCollection(MongoDatabase database, String collectionName) {
        database.getCollection(collectionName).drop();
        return Map.of("dropped", collectionName);
    }

    private Object save(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Map<String, Object> map = asMap(operation.getPayload(), "payload");
        Object idVal = map.get("_id");
        Document doc = new Document(map);

        if (idVal != null && !idVal.toString().isBlank()) {
            Object resolvedId = resolveId(idVal.toString());
            collection.replaceOne(
                    new Document("_id", resolvedId),
                    doc,
                    new ReplaceOptions().upsert(true));
            return Map.of("_id", idVal.toString());
        } else {
            String newId = UUID.randomUUID().toString();
            doc.put("_id", newId);
            collection.insertOne(doc);
            return Map.of("_id", newId);
        }
    }

    private Object find(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Document filter = buildFindFilter(operation);
        Integer limit = operation.getLimit();
        var query = collection.find(filter).sort(new Document("_id", 1));
        int defaultLimit = resolveDefaultLimit(operation);
        int maxLimit = resolveMaxLimit(operation, defaultLimit);
        int effectiveLimit = resolveEffectiveLimit(limit, defaultLimit, maxLimit);
        query = query.limit(effectiveLimit);
        List<Map<String, Object>> results = new ArrayList<>();
        for (Document doc : query) {
            Map<String, Object> row = new LinkedHashMap<>(doc);
            if (row.get("_id") instanceof ObjectId oid) {
                row.put("_id", oid.toHexString());
            }
            results.add(row);
        }
        return results;
    }

    private Object listSchema(MongoDatabase database) {
        List<Map<String, Object>> tables = new ArrayList<>();
        for (String collectionName : database.listCollectionNames()) {
            List<Map<String, Object>> columns = new ArrayList<>();
            Document sample = database.getCollection(collectionName).find().first();
            if (sample != null) {
                for (Map.Entry<String, Object> field : sample.entrySet()) {
                    String type = field.getValue() == null ? "unknown"
                            : field.getValue().getClass().getSimpleName();
                    columns.add(Map.of("name", field.getKey(), "type", type));
                }
            }
            tables.add(Map.of("name", collectionName, "columns", columns));
        }
        return tables;
    }

    private Object insert(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Object payload = operation.getPayload();
        if (payload == null) {
            throw new IllegalArgumentException("Missing operation.payload for insert");
        }
        if (payload instanceof List<?> payloadList) {
            List<Document> documents = new ArrayList<>();
            for (Object item : payloadList) {
                documents.add(toDocument(asMap(item, "operation.payload")));
            }
            InsertManyResult result = collection.insertMany(documents);
            return Map.of("insertedCount", result.getInsertedIds().size());
        }
        Map<String, Object> map = asMap(payload, "operation.payload");
        InsertOneResult result = collection.insertOne(new Document(map));
        return Map.of("insertedId", result.getInsertedId());
    }

    private Object update(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Document filter = toFilterDocument(operation.getFilter());
        Map<String, Object> updateFields = asMap(operation.getPayload(), "operation.payload");
        UpdateResult result = collection.updateMany(filter, new Document("$set", updateFields));
        return Map.of(
                "matchedCount", result.getMatchedCount(),
                "modifiedCount", result.getModifiedCount()
        );
    }

    private Object delete(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Document filter = toFilterDocument(operation.getFilter());
        DeleteResult result = collection.deleteMany(filter);
        return Map.of("deletedCount", result.getDeletedCount());
    }

    // Convierte un string _id de 24 hex chars a ObjectId para compatibilidad
    // con documentos creados por Spring Data MongoDB (que usa ObjectId nativo).
    // IDs UUID (36 chars con guiones) se devuelven sin cambio.
    private Object resolveId(String s) {
        if (s != null && s.length() == 24) {
            try { return new ObjectId(s); } catch (IllegalArgumentException ignored) {}
        }
        return s;
    }

    private Document toDocument(Map<String, Object> map) {
        if (map == null) {
            return new Document();
        }
        return new Document(map);
    }

    private Document toFilterDocument(Map<String, Object> map) {
        Document doc = toDocument(map);
        Object id = doc.get("_id");
        if (id instanceof String s) doc.put("_id", resolveId(s));
        return doc;
    }

    private Document buildFindFilter(RepositoryOperationDto operation) {
        Document baseFilter = toFilterDocument(operation.getFilter());
        String cursor = operation.getCursor();
        if (cursor == null || cursor.isBlank()) {
            return baseFilter;
        }
        ObjectId objectId = parseCursor(cursor);
        Document cursorFilter = new Document("_id", new Document("$gt", objectId));
        if (baseFilter.isEmpty()) {
            return cursorFilter;
        }
        if (baseFilter.containsKey("_id")) {
            return new Document("$and", Arrays.asList(baseFilter, cursorFilter));
        }
        baseFilter.put("_id", new Document("$gt", objectId));
        return baseFilter;
    }

    private ObjectId parseCursor(String cursor) {
        try {
            return new ObjectId(cursor);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid cursor; expected Mongo ObjectId hex string");
        }
    }

    private int resolveDefaultLimit(RepositoryOperationDto operation) {
        Integer provided = operation.getDefaultLimit();
        if (provided != null && provided > 0) {
            return Math.min(provided, ABSOLUTE_MAX_LIMIT);
        }
        return DEFAULT_LIMIT;
    }

    private int resolveMaxLimit(RepositoryOperationDto operation, int defaultLimit) {
        Integer provided = operation.getMaxLimit();
        if (provided != null && provided > 0) {
            return Math.min(provided, ABSOLUTE_MAX_LIMIT);
        }
        return Math.min(MAX_LIMIT, ABSOLUTE_MAX_LIMIT);
    }

    private int resolveEffectiveLimit(Integer limit, int defaultLimit, int maxLimit) {
        int effective = defaultLimit;
        if (limit != null && limit > 0) {
            effective = limit;
        }
        return Math.min(effective, maxLimit);
    }

    private Map<String, Object> asMap(Object payload, String name) {
        if (payload instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> casted = (Map<String, Object>) map;
            return casted;
        }
        throw new IllegalArgumentException("Expected " + name + " to be an object");
    }
}
