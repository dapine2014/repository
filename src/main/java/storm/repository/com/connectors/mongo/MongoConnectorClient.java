package storm.repository.com.connectors.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

public final class MongoConnectorClient {
    private MongoConnectorClient() {
    }

    public static MongoClient createClient(MongoConnectorConfig config) {
        return MongoClients.create(config.uri());
    }

    public static void validateConnection(MongoConnectorConfig config) {
        try (MongoClient client = createClient(config)) {
            client.getDatabase(config.database()).runCommand(new Document("ping", 1));
        }
    }
}
