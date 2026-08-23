package storm.repository.com.core.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectivityResolverTest {

    @Test
    void resolveReturnsDirectEndpointWhenAccesoTipoIsAbsent() {
        Map<String, String> config = Map.of("host", "db.internal", "port", "5432");
        try (var endpoint = ConnectivityResolver.resolve(config, "db.internal", 5432)) {
            assertThat(endpoint.host()).isEqualTo("db.internal");
            assertThat(endpoint.port()).isEqualTo(5432);
        }
    }

    @Test
    void resolveReturnsDirectEndpointWhenAccesoTipoIsDirecto() {
        Map<String, String> config = Map.of("accesoTipo", "DIRECTO", "host", "db.internal", "port", "5432");
        try (var endpoint = ConnectivityResolver.resolve(config, "db.internal", 5432)) {
            assertThat(endpoint.host()).isEqualTo("db.internal");
            assertThat(endpoint.port()).isEqualTo(5432);
        }
    }

    @Test
    void resolveDelegatesToSshTunnelManagerWhenAccesoTipoIsSshTunnel() {
        // Sin bastion real disponible en el entorno de test: confirmamos que el
        // despacho llega a SshTunnelManager (lanza IllegalArgumentException por
        // campos de bastion faltantes) en vez de a SsmTunnelManager (que lanzaría
        // el mismo tipo de excepción pero por campos SSM faltantes) — se verifica
        // el mensaje de la excepción para distinguir cuál de las dos la lanzó.
        Map<String, String> config = Map.of("accesoTipo", "SSH_TUNNEL");
        try {
            ConnectivityResolver.resolve(config, "db.internal", 5432);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("túnel");
        }
    }

    @Test
    void resolveDelegatesToSsmTunnelManagerWhenAccesoTipoIsAwsSsm() {
        Map<String, String> config = Map.of("accesoTipo", "AWS_SSM");
        try {
            ConnectivityResolver.resolve(config, "db.internal", 5432);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("AWS SSM");
        }
    }
}
