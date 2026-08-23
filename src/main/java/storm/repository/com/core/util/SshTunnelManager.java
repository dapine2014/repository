package storm.repository.com.core.util;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public final class SshTunnelManager {

    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MS = 1000;
    private static final int CONNECT_TIMEOUT_MS = 10_000;

    private SshTunnelManager() {
    }

    public interface ResolvedEndpoint extends AutoCloseable {
        String host();
        int port();
        @Override void close();
    }

    /**
     * Si config.accesoTipo != "SSH_TUNNEL", devuelve un ResolvedEndpoint directo
     * (host/port tal cual, close() no-op). Si es SSH_TUNNEL, abre un túnel SSH
     * al bastion descrito en config (bastionHost/bastionPuerto/bastionUsuario/
     * bastionLlavePem) y devuelve un endpoint apuntando a 127.0.0.1:<puerto local>.
     */
    public static ResolvedEndpoint resolve(Map<String, String> config, String remoteHost, int remotePort) {
        if (!"SSH_TUNNEL".equals(config.get("accesoTipo"))) {
            return direct(remoteHost, remotePort);
        }

        String bastionHost = required(config, "bastionHost");
        int bastionPort = Integer.parseInt(config.getOrDefault("bastionPuerto", "22"));
        String bastionUser = required(config, "bastionUsuario");
        String pemKey = required(config, "bastionLlavePem");

        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            Session session = null;
            try {
                JSch jsch = new JSch();
                jsch.addIdentity("storm-bastion", pemKey.getBytes(StandardCharsets.UTF_8), null, null);
                session = jsch.getSession(bastionUser, bastionHost, bastionPort);
                session.setConfig("StrictHostKeyChecking", "no");
                session.connect(CONNECT_TIMEOUT_MS);
                int localPort = session.setPortForwardingL(0, remoteHost, remotePort);
                log.info("Túnel SSH abierto bastion={}:{} -> {}:{} (puerto local {})",
                        bastionHost, bastionPort, remoteHost, remotePort, localPort);
                return tunneled(session, localPort);
            } catch (Exception e) {
                lastError = e;
                if (session != null && session.isConnected()) {
                    session.disconnect();
                }
                log.warn("Intento {}/{} de túnel SSH a {} falló: {}", attempt, MAX_ATTEMPTS, bastionHost, e.getMessage());
                if (attempt < MAX_ATTEMPTS) {
                    sleep(RETRY_BACKOFF_MS);
                }
            }
        }
        throw new IllegalStateException(classifyError(bastionHost, bastionPort, lastError), lastError);
    }

    /**
     * Clasifica la causa del fallo de túnel SSH en un mensaje específico y
     * legible por el usuario final (que puede estar mirando un spinner en el
     * wizard de storm-ui), en lugar de un mensaje genérico único para todos
     * los casos. Réplica en Java del espíritu de `_friendly_tunnel_error()`
     * en el sandbox Python (`tunnel_manager.py`).
     */
    private static String classifyError(String bastionHost, int bastionPort, Exception lastError) {
        String target = bastionHost + ":" + bastionPort;
        String detail = lastError == null ? "" : String.valueOf(lastError.getMessage());
        String lower = detail.toLowerCase();

        if (lower.contains("auth fail") || lower.contains("authentication failed") || lower.contains("permission denied")) {
            return "No se pudo autenticar contra el bastion SSH " + target
                    + " — verifique el usuario y la llave privada configurados.";
        }
        if (lower.contains("unknownhostexception") || lower.contains("unknown host")
                || lower.contains("nodename nor servname provided") || lower.contains("name or service not known")) {
            return "No se pudo resolver el host del bastion SSH " + target
                    + " — verifique que el nombre/dirección sea correcto.";
        }
        if (lower.contains("connection refused")) {
            return "Conexión rechazada por el bastion SSH " + target
                    + " — verifique que el servicio SSH esté activo y el puerto sea correcto.";
        }
        if (lower.contains("timeout") || lower.contains("timed out")) {
            return "Tiempo de espera agotado al conectar con el bastion SSH " + target
                    + " — verifique conectividad de red y reglas de firewall/security group.";
        }
        return "No se pudo establecer el túnel SSH al bastion " + target;
    }

    private static ResolvedEndpoint direct(String host, int port) {
        return new ResolvedEndpoint() {
            @Override public String host() { return host; }
            @Override public int port() { return port; }
            @Override public void close() { /* no-op: no hay túnel que cerrar */ }
        };
    }

    private static ResolvedEndpoint tunneled(Session session, int localPort) {
        return new ResolvedEndpoint() {
            @Override public String host() { return "127.0.0.1"; }
            @Override public int port() { return localPort; }
            @Override public void close() {
                try {
                    session.delPortForwardingL(localPort);
                } catch (Exception ignored) {
                }
                session.disconnect();
            }
        };
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Falta configuración de túnel requerida: " + key);
        }
        return value;
    }
}
