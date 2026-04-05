package storm.repository.com.connectors.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import java.util.List;
import java.util.Map;

@Component
public class MongoConnectorExecutor implements RepositoryConnectorExecutor {
    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 5000;
    private static final int ABSOLUTE_MAX_LIMIT = 100000;
    @Override
    public String connectorId() {
        return "mongo";
    }

    @Override
    public Object execute(RepositoryOperationDto operation, Map<String, String> config) {
        MongoConnectorConfig mongoConfig = new MongoConnectorConfig(new ConnectorConfig(config));
        validateOperation(operation);

        try (MongoClient client = MongoConnectorClient.createClient(mongoConfig)) {
            MongoDatabase database = client.getDatabase(mongoConfig.database());
            MongoCollection<Document> collection = database.getCollection(operation.getCollection());
            String method = operation.getMethod().toLowerCase();

            return switch (method) {
                case "find" -> find(collection, operation);
                case "insert" -> insert(collection, operation);
                case "update" -> update(collection, operation);
                case "delete" -> delete(collection, operation);
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
        if (operation.getCollection() == null || operation.getCollection().isBlank()) {
            throw new IllegalArgumentException("Missing operation.collection");
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
        List<Document> results = query.into(new ArrayList<>());
        String nextCursor = extractNextCursor(results);
        long totalCount = collection.countDocuments(toDocument(operation.getFilter()));
        return Map.of(
                "items", results,
                "nextCursor", nextCursor,
                "totalCount", totalCount
        );
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
        Document filter = toDocument(operation.getFilter());
        Map<String, Object> updateFields = asMap(operation.getPayload(), "operation.payload");
        UpdateResult result = collection.updateMany(filter, new Document("$set", updateFields));
        return Map.of(
                "matchedCount", result.getMatchedCount(),
                "modifiedCount", result.getModifiedCount()
        );
    }

    private Object delete(MongoCollection<Document> collection, RepositoryOperationDto operation) {
        Document filter = toDocument(operation.getFilter());
        DeleteResult result = collection.deleteMany(filter);
        return Map.of("deletedCount", result.getDeletedCount());
    }

    private Document toDocument(Map<String, Object> map) {
        if (map == null) {
            return new Document();
        }
        return new Document(map);
    }

    private Document buildFindFilter(RepositoryOperationDto operation) {
        Document baseFilter = toDocument(operation.getFilter());
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

    private String extractNextCursor(List<Document> results) {
        if (results == null || results.isEmpty()) {
            return null;
        }
        Document last = results.get(results.size() - 1);
        Object id = last.get("_id");
        if (id instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        return id == null ? null : id.toString();
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
