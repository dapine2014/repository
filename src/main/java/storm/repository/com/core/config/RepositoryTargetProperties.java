package storm.repository.com.core.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.repository")
public class RepositoryTargetProperties {
    private Map<String, TargetConfig> targets = new HashMap<>();

    @Data
    public static class TargetConfig {
        private String connectorId;
        private Map<String, String> config = new HashMap<>();
    }
}
