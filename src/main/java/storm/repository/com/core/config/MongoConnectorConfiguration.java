package storm.repository.com.core.config;

import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import storm.repository.com.connectors.mongo.MongoConnectorClient;
import storm.repository.com.connectors.mongo.MongoConnectorConfig;
import storm.repository.com.core.api.ConnectorConfig;

import java.util.Map;

@Configuration
public class MongoConnectorConfiguration {
    @Value("${app.mongo.uri}")
    private String uri;

    @Value("${app.mongo.database}")
    private String database;

    @Bean
    public MongoConnectorConfig mongoConnectorConfig() {
        Map<String, String> props = Map.of(
                MongoConnectorConfig.URI, uri,
                MongoConnectorConfig.DATABASE, database
        );
        return new MongoConnectorConfig(new ConnectorConfig(props));
    }

    @Bean
    public MongoClient mongoClient(MongoConnectorConfig config) {
        return MongoConnectorClient.createClient(config);
    }
}
