package storm.repository.com.core.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SsmTunnelManagerTest {

    /**
     * Regresión: la revisión final de rama encontró, ejecutando el binario real
     * session-manager-plugin, que argv[5] llevaba solo el mapa interno de
     * Parameters en vez de la request completa de StartSession
     * ({"Target":...,"DocumentName":...,"Parameters":...}) — el plugin fallaba
     * con un panic inmediato ante cualquier túnel AWS SSM real. Este test fija
     * la forma del argv sin necesitar invocar el binario ni AWS real.
     */
    @Test
    void buildPluginArgsIncluyeLaRequestCompletaDeStartSessionEnElArgumentoSeis() {
        List<String> args = SsmTunnelManager.buildPluginArgs(
                "{\"SessionId\":\"s-abc\"}", "us-east-1", "i-0123456789abcdef0",
                "{\"host\":[\"10.0.1.20\"],\"portNumber\":[\"5432\"],\"localPortNumber\":[\"54321\"]}",
                "https://ssm.us-east-1.amazonaws.com");

        assertThat(args).hasSize(7);
        assertThat(args.get(0)).isEqualTo("session-manager-plugin");
        assertThat(args.get(3)).isEqualTo("StartSession");

        String startSessionRequestJson = args.get(5);
        assertThat(startSessionRequestJson)
                .contains("\"Target\":\"i-0123456789abcdef0\"")
                .contains("\"DocumentName\":\"AWS-StartPortForwardingToRemoteHost\"")
                .contains("\"Parameters\":{\"host\":[\"10.0.1.20\"]");
    }

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
