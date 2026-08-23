package storm.repository.com.core.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SsmTunnelManagerTest {

    @Test
    void resolveReturnsDirectEndpointWhenAccesoTipoIsNotAwsSsm() {
        Map<String, String> config = Map.of("host", "db.internal", "port", "5432");
        try (var endpoint = SsmTunnelManager.resolve(config, "db.internal", 5432)) {
            assertThat(endpoint.host()).isEqualTo("db.internal");
            assertThat(endpoint.port()).isEqualTo(5432);
        }
    }

    @Test
    void resolveThrowsWhenAwsSsmRequestedButFieldsMissing() {
        Map<String, String> config = Map.of("accesoTipo", "AWS_SSM");
        assertThatThrownBy(() -> SsmTunnelManager.resolve(config, "db.internal", 5432))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resolveThrowsWhenAwsSsmRequestedWithRegionAndInstanceIdButNoCredentials() {
        Map<String, String> config = Map.of(
                "accesoTipo", "AWS_SSM",
                "region", "us-east-1",
                "ssmInstanceId", "i-0123456789abcdef0");
        assertThatThrownBy(() -> SsmTunnelManager.resolve(config, "db.internal", 5432))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
