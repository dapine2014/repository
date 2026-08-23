package storm.repository.com.core.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class kafkaProviderConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String boostrapServer;
    @Value("${spring.kafka.properties.security.protocol:PLAINTEXT}")
    private String securityProtocol;
    @Value("${spring.kafka.properties.ssl.truststore.location:}")
    private String sslTruststoreLocation;
    @Value("${spring.kafka.properties.ssl.truststore.password:}")
    private String sslTruststorePassword;

    public Map<String,Object> producerConfig(){
        Map<String,Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        addSslProps(properties);
        return properties;
    }

    private void addSslProps(Map<String, Object> props) {
        if (!"PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            props.put("security.protocol", securityProtocol);
            if (!sslTruststoreLocation.isBlank()) {
                props.put("ssl.truststore.location", sslTruststoreLocation);
            }
            if (!sslTruststorePassword.isBlank()) {
                props.put("ssl.truststore.password", sslTruststorePassword);
            }
            props.put("ssl.endpoint.identification.algorithm", "");
        }
    }

    @Bean
    public ProducerFactory<String, String> providerFactory(){

        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory){

        return new KafkaTemplate<>(producerFactory);
    }

}
