package storm.repository.com.core.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topic.repository-query}")
    private String queryTopic;

    @Value("${app.kafka.topic.repository-response}")
    private String responseTopic;

    @Bean
    public NewTopic repositoryQueryTopic(){
        HashMap<String, String> configTopic = new HashMap<>();
        configTopic.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE); //borrado de mensajes
        configTopic.put(TopicConfig.RETENTION_MS_CONFIG,"86400000"); //tiempo de retencion los mensajes
        configTopic.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824"); //tamaño maximo del segmento - 1GB
        configTopic.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,"1073741824"); //tamaño maximo del mensaje

        return TopicBuilder.name(queryTopic)
                .partitions(1)
                .replicas(1)
                .configs(configTopic)
                .build();
    }

    @Bean
    public NewTopic repositoryResponseTopic(){
        HashMap<String, String> configTopic = new HashMap<>();
        configTopic.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE); //borrado de mensajes
        configTopic.put(TopicConfig.RETENTION_MS_CONFIG,"86400000"); //tiempo de retencion los mensajes
        configTopic.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824"); //tamaño maximo del segmento - 1GB
        configTopic.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,"1073741824"); //tamaño maximo del mensaje

        return TopicBuilder.name(responseTopic)
                .partitions(1)
                .replicas(1)
                .configs(configTopic)
                .build();
    }
}
