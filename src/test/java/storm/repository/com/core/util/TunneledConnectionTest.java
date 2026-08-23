package storm.repository.com.core.util;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;

class TunneledConnectionTest {

    @Test
    void closeDelegatesAndClosesTunnel() throws SQLException {
        Connection delegate = mock(Connection.class);
        AtomicInteger tunnelCloseCount = new AtomicInteger(0);
        AutoCloseable tunnel = tunnelCloseCount::incrementAndGet;

        Connection wrapped = TunneledConnection.wrap(delegate, tunnel);
        wrapped.close();

        verify(delegate, times(1)).close();
        assertThat(tunnelCloseCount.get()).isEqualTo(1);
    }

    @Test
    void closeStillClosesTunnelWhenDelegateCloseThrows() throws SQLException {
        Connection delegate = mock(Connection.class);
        SQLException boom = new SQLException("connection reset by peer");
        doThrow(boom).when(delegate).close();

        AtomicInteger tunnelCloseCount = new AtomicInteger(0);
        AutoCloseable tunnel = tunnelCloseCount::incrementAndGet;

        Connection wrapped = TunneledConnection.wrap(delegate, tunnel);

        assertThatThrownBy(wrapped::close)
                .isSameAs(boom);

        assertThat(tunnelCloseCount.get())
                .as("el túnel debe cerrarse incluso si delegate.close() lanza")
                .isEqualTo(1);
    }

    @Test
    void nonCloseMethodsDoNotCloseTunnel() throws SQLException {
        Connection delegate = mock(Connection.class);
        AtomicInteger tunnelCloseCount = new AtomicInteger(0);
        AutoCloseable tunnel = tunnelCloseCount::incrementAndGet;

        Connection wrapped = TunneledConnection.wrap(delegate, tunnel);
        wrapped.isClosed();
        wrapped.getAutoCommit();

        assertThat(tunnelCloseCount.get()).isZero();
    }
}
