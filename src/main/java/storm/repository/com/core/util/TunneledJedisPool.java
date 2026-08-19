package storm.repository.com.core.util;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * JedisPool no es una interfaz, así que no se puede envolver con un proxy
 * dinámico como TunneledConnection — se extiende directamente para cerrar
 * el túnel SSH cuando se cierra el pool.
 */
public final class TunneledJedisPool extends JedisPool {

    private final AutoCloseable tunnel;

    public TunneledJedisPool(JedisPoolConfig poolConfig, String host, int port, int timeout,
                              String password, int database, AutoCloseable tunnel) {
        super(poolConfig, host, port, timeout, password, database);
        this.tunnel = tunnel;
    }

    @Override
    public void close() {
        super.close();
        try {
            tunnel.close();
        } catch (Exception ignored) {
        }
    }
}
