package storm.repository.com.core.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;

/**
 * Envuelve una Connection JDBC en un proxy dinámico que, además de delegar
 * todas las llamadas a la conexión real, cierra el túnel SSH (si lo hay)
 * cuando se llama a close() — para que el ciclo de vida del túnel coincida
 * exactamente con el de la conexión que lo usa.
 */
public final class TunneledConnection {

    private TunneledConnection() {
    }

    public static Connection wrap(Connection delegate, AutoCloseable tunnel) {
        InvocationHandler handler = (proxy, method, args) -> {
            try {
                Object result = method.invoke(delegate, args);
                if ("close".equals(method.getName())) {
                    closeTunnel(tunnel);
                }
                return result;
            } catch (InvocationTargetException e) {
                if ("close".equals(method.getName())) {
                    closeTunnel(tunnel);
                }
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                handler);
    }

    private static void closeTunnel(AutoCloseable tunnel) {
        try {
            tunnel.close();
        } catch (Exception ignored) {
        }
    }
}
