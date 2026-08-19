package storm.repository.com.core.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SshTunnelManagerTest {

    @Test
    void resolveReturnsDirectEndpointWhenAccesoTipoIsNotSshTunnel() {
        Map<String, String> config = Map.of("host", "db.internal", "port", "5432");

        try (var endpoint = SshTunnelManager.resolve(config, "db.internal", 5432)) {
            assertThat(endpoint.host()).isEqualTo("db.internal");
            assertThat(endpoint.port()).isEqualTo(5432);
        }
    }

    @Test
    void resolveThrowsWhenSshTunnelRequestedButBastionFieldsMissing() {
        Map<String, String> config = Map.of("accesoTipo", "SSH_TUNNEL");

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> SshTunnelManager.resolve(config, "db.internal", 5432));
    }
}
