package storm.repository.com.core.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RepositoryTargetRegistry {
    private final RepositoryTargetProperties properties;

    public RepositoryTargetRegistry(RepositoryTargetProperties properties) {
        this.properties = properties;
    }

    public ResolvedTarget resolve(String targetRef) {
        if (targetRef == null || targetRef.isBlank()) {
            return null;
        }
        RepositoryTargetProperties.TargetConfig target = properties.getTargets().get(targetRef);
        if (target == null) {
            throw new IllegalArgumentException("Unknown repository target: " + targetRef);
        }
        return new ResolvedTarget(
                target.getConnectorId(),
                target.getConfig() != null ? new HashMap<>(target.getConfig()) : Map.of());
    }

    public record ResolvedTarget(
            String connectorId,
            Map<String, String> config) {
    }
}
