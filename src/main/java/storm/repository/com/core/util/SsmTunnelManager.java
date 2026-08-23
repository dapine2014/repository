package storm.repository.com.core.util;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.StartSessionRequest;
import software.amazon.awssdk.services.ssm.model.StartSessionResponse;
import software.amazon.awssdk.services.ssm.model.TerminateSessionRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

@Slf4j
public final class SsmTunnelManager {

    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MS = 1000;
    private static final long READY_TIMEOUT_MS = 15_000;
    private static final long READY_POLL_INTERVAL_MS = 200;
    private static final int STDERR_TAIL_LINES = 50;

    private SsmTunnelManager() {
    }

    public interface ResolvedEndpoint extends AutoCloseable {
        String host();
        int port();
        @Override void close();
    }

    /**
     * Si config.accesoTipo != "AWS_SSM", devuelve un ResolvedEndpoint directo
     * (host/port tal cual, close() no-op). Si es AWS_SSM, abre una sesión SSM
     * Session Manager hacia la instancia descrita en config (region/ssmInstanceId/
     * ssmAccessKeyId/ssmSecretAccessKey), lanza el binario session-manager-plugin
     * como subproceso, y devuelve un endpoint apuntando a 127.0.0.1:<puerto local>.
     */
    public static ResolvedEndpoint resolve(Map<String, String> config, String remoteHost, int remotePort) {
        if (!"AWS_SSM".equals(config.get("accesoTipo"))) {
            return direct(remoteHost, remotePort);
        }

        String region = required(config, "region");
        String instanceId = required(config, "ssmInstanceId");
        String accessKeyId = required(config, "ssmAccessKeyId");
        String secretAccessKey = required(config, "ssmSecretAccessKey");

        Exception lastError = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return openTunnel(region, instanceId, accessKeyId, secretAccessKey, remoteHost, remotePort);
            } catch (Exception e) {
                lastError = e;
                log.warn("Intento {}/{} de túnel AWS SSM a {} falló: {}", attempt, MAX_ATTEMPTS, instanceId, e.getMessage());
                if (attempt < MAX_ATTEMPTS) {
                    sleep(RETRY_BACKOFF_MS);
                }
            }
        }
        throw new IllegalStateException(classifyError(instanceId, region, lastError), lastError);
    }

    private static ResolvedEndpoint openTunnel(String region, String instanceId, String accessKeyId,
                                                String secretAccessKey, String remoteHost, int remotePort) {
        SsmClient ssmClient = SsmClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                .build();

        int localPort = pickFreePort();
        String parametersJson = String.format(
                "{\"host\":[\"%s\"],\"portNumber\":[\"%d\"],\"localPortNumber\":[\"%d\"]}",
                remoteHost, remotePort, localPort);

        StartSessionResponse response;
        try {
            response = ssmClient.startSession(StartSessionRequest.builder()
                    .target(instanceId)
                    .documentName("AWS-StartPortForwardingToRemoteHost")
                    .parameters(Map.of(
                            "host", List.of(remoteHost),
                            "portNumber", List.of(String.valueOf(remotePort)),
                            "localPortNumber", List.of(String.valueOf(localPort))))
                    .build());
        } catch (Exception e) {
            ssmClient.close();
            throw e;
        }

        String responseJson = String.format(
                "{\"SessionId\":\"%s\",\"TokenValue\":\"%s\",\"StreamUrl\":\"%s\"}",
                response.sessionId(), response.tokenValue(), response.streamUrl());

        ProcessBuilder pb = new ProcessBuilder(
                "session-manager-plugin", responseJson, region, "StartSession", "",
                parametersJson, ssmClient.serviceClientConfiguration().endpointOverride()
                        .map(Object::toString)
                        .orElse("https://ssm." + region + ".amazonaws.com"));
        pb.redirectErrorStream(false);

        Process process;
        try {
            process = pb.start();
        } catch (Exception e) {
            terminateSession(ssmClient, response.sessionId());
            ssmClient.close();
            throw new IllegalStateException("No se pudo lanzar session-manager-plugin", e);
        }

        // El plugin puede escribir a stdout/stderr durante toda la vida del túnel
        // (reconexiones, warnings). Si nadie drena esos pipes, el buffer del SO
        // (~64KB en Linux) se llena y el subproceso se bloquea en el write,
        // congelando el túnel en silencio — en este pod compartido de larga
        // duración eso puede retener indefinidamente un hilo del pool de
        // consumidores de Kafka. Hilos daemon drenan ambos continuamente hacia
        // un buffer acotado, usado solo para el mensaje de error si falla el arranque.
        startStreamDrain(process.getInputStream());
        Deque<String> stderrTail = startStreamDrain(process.getErrorStream());

        if (!waitForPort(localPort, READY_TIMEOUT_MS)) {
            process.destroy();
            terminateSession(ssmClient, response.sessionId());
            ssmClient.close();
            String stderr = tailToString(stderrTail);
            throw new IllegalStateException(
                    "El plugin session-manager-plugin no abrió el puerto local " + localPort
                            + " en " + READY_TIMEOUT_MS + "ms (instancia=" + instanceId + "): " + stderr);
        }

        log.info("Túnel AWS SSM abierto instancia={} región={} -> {}:{} (puerto local {}, sesión {})",
                instanceId, region, remoteHost, remotePort, localPort, response.sessionId());
        return tunneled(process, localPort, ssmClient, response.sessionId());
    }

    /**
     * Clasifica la causa del fallo de túnel AWS SSM en un mensaje específico y
     * legible por el usuario final, en lugar de un mensaje genérico único para
     * todos los casos. Réplica en Java del espíritu de `_friendly_ssm_error()`
     * en el sandbox Python (`ssm_tunnel_manager.py`).
     */
    private static String classifyError(String instanceId, String region, Exception lastError) {
        String detail = lastError == null ? "" : String.valueOf(lastError.getMessage());
        String lower = detail.toLowerCase();

        if (lower.contains("accessdenied") || lower.contains("invalidclienttokenid")
                || lower.contains("unrecognizedclientexception") || lower.contains("signaturedoesnotmatch")) {
            return "No se pudo autenticar contra AWS (región " + region + ") — "
                    + "verifique el Access Key ID y la Secret Access Key configurados.";
        }
        if (lower.contains("targetnotconnected")) {
            return "La instancia " + instanceId + " no tiene el SSM Agent registrado/conectado — "
                    + "verifique que el Agent esté corriendo y la instancia tenga el rol IAM correcto.";
        }
        if (lower.contains("invalidinstanceid")) {
            return "La instancia " + instanceId + " no existe o no es visible en la región " + region + ".";
        }
        return "No se pudo establecer el túnel AWS SSM a la instancia " + instanceId
                + " (región " + region + "): " + detail;
    }

    private static ResolvedEndpoint direct(String host, int port) {
        return new ResolvedEndpoint() {
            @Override public String host() { return host; }
            @Override public int port() { return port; }
            @Override public void close() { /* no-op: no hay túnel que cerrar */ }
        };
    }

    private static ResolvedEndpoint tunneled(Process process, int localPort, SsmClient ssmClient, String sessionId) {
        return new ResolvedEndpoint() {
            @Override public String host() { return "127.0.0.1"; }
            @Override public int port() { return localPort; }
            @Override public void close() {
                try {
                    process.destroy();
                    if (!process.waitFor(Duration.ofSeconds(5).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        process.destroyForcibly();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    terminateSession(ssmClient, sessionId);
                    ssmClient.close();
                }
            }
        };
    }

    private static void terminateSession(SsmClient ssmClient, String sessionId) {
        try {
            ssmClient.terminateSession(TerminateSessionRequest.builder().sessionId(sessionId).build());
        } catch (Exception ignored) {
        }
    }

    private static int pickFreePort() {
        try (var socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo asignar un puerto local libre", e);
        }
    }

    private static boolean waitForPort(int port, long timeoutMs) {
        Instant deadline = Instant.now().plusMillis(timeoutMs);
        while (Instant.now().isBefore(deadline)) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 1000);
                return true;
            } catch (Exception e) {
                sleep(READY_POLL_INTERVAL_MS);
            }
        }
        return false;
    }

    /**
     * Lanza un hilo daemon que drena `stream` línea a línea hasta EOF, guardando
     * solo las últimas STDERR_TAIL_LINES en un Deque acotado — evita que el pipe
     * se llene y bloquee al subproceso, y conserva contexto útil para el mensaje
     * de error si el túnel falla al arrancar.
     */
    private static Deque<String> startStreamDrain(InputStream stream) {
        Deque<String> tail = new ArrayDeque<>();
        Thread drainThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (tail) {
                        tail.addLast(line);
                        if (tail.size() > STDERR_TAIL_LINES) {
                            tail.removeFirst();
                        }
                    }
                }
            } catch (IOException ignored) {
                // El stream se cierra cuando el proceso termina — fin normal del hilo.
            }
        });
        drainThread.setDaemon(true);
        drainThread.setName("ssm-tunnel-stream-drain");
        drainThread.start();
        return tail;
    }

    private static String tailToString(Deque<String> tail) {
        synchronized (tail) {
            return String.join("\n", tail);
        }
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
            throw new IllegalArgumentException("Falta configuración de AWS SSM requerida: " + key);
        }
        return value;
    }
}
