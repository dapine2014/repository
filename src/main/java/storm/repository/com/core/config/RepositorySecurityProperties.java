package storm.repository.com.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.security")
public class RepositorySecurityProperties {
    /**
     * Only enable for trusted internal callers. Keeping this false avoids sending URI/keys in Kafka payloads.
     */
    private boolean allowInlineSensitiveConfig = false;
}
