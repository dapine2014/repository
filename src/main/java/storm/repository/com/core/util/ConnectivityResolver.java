package storm.repository.com.core.util;

import java.util.Map;

/**
 * Punto único de despacho entre los mecanismos de conectividad remota
 * disponibles (DIRECTO, SSH_TUNNEL, AWS_SSM). No implementa ningún túnel
 * por sí mismo — delega en SshTunnelManager o SsmTunnelManager según
 * config.accesoTipo, cada uno responsable de su propio mecanismo.
 */
public final class ConnectivityResolver {

    private ConnectivityResolver() {
    }

    public interface ResolvedEndpoint extends AutoCloseable {
        String host();
        int port();
        @Override void close();
    }

    public static ResolvedEndpoint resolve(Map<String, String> config, String remoteHost, int remotePort) {
        if ("AWS_SSM".equals(config.get("accesoTipo"))) {
            SsmTunnelManager.ResolvedEndpoint endpoint = SsmTunnelManager.resolve(config, remoteHost, remotePort);
            return adapt(endpoint);
        }
        SshTunnelManager.ResolvedEndpoint endpoint = SshTunnelManager.resolve(config, remoteHost, remotePort);
        return adapt(endpoint);
    }

    private static ResolvedEndpoint adapt(AutoCloseable inner) {
        var typed = (Object) inner;
        String host;
        int port;
        if (typed instanceof SshTunnelManager.ResolvedEndpoint ssh) {
            host = ssh.host();
            port = ssh.port();
        } else {
            SsmTunnelManager.ResolvedEndpoint ssm = (SsmTunnelManager.ResolvedEndpoint) typed;
            host = ssm.host();
            port = ssm.port();
        }
        return new ResolvedEndpoint() {
            @Override public String host() { return host; }
            @Override public int port() { return port; }
            @Override public void close() {
                try {
                    inner.close();
                } catch (Exception e) {
                    throw new IllegalStateException("Error al cerrar el endpoint resuelto", e);
                }
            }
        };
    }
}
