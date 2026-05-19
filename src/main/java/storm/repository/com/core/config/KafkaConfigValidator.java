package storm.repository.com.core.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class KafkaConfigValidator implements ApplicationRunner {
    @Value("${spring.kafka.bootstrap-servers:}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:}")
    private String groupId;

    @Value("${app.kafka.topic.repository-query:}")
    private String queryTopic;

    @Value("${app.kafka.topic.repository-response:}")
    private String responseTopic;

    @Value("${spring.kafka.properties.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.ssl.truststore.location:}")
    private String sslTruststoreLocation;

    @Value("${spring.kafka.properties.ssl.truststore.password:}")
    private String sslTruststorePassword;

    @Override
    public void run(ApplicationArguments args) {
        validateNotBlank(bootstrapServers, "spring.kafka.bootstrap-servers");
        validateNotBlank(groupId, "spring.kafka.consumer.group-id");
        validateNotBlank(queryTopic, "app.kafka.topic.repository-query");
        validateNotBlank(responseTopic, "app.kafka.topic.repository-response");

        validateKafkaConnection();
        if (queryTopic.equals(responseTopic)) {
            log.warn("Kafka topics are the same for request/response: {}", queryTopic);
        }
    }

    private void validateNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required Kafka property: " + name);
        }
    }

    private void validateKafkaConnection() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        if ("SSL".equalsIgnoreCase(securityProtocol)) {
            if (!sslTruststoreLocation.isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
            }
            if (!sslTruststorePassword.isBlank()) {
                props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
            }
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        }

        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            log.info("Kafka connection OK bootstrap={} protocol={}", bootstrapServers, securityProtocol);
        } catch (Exception ex) {
            throw new IllegalStateException("Kafka connection check failed for " + bootstrapServers, ex);
        }
    }
}
