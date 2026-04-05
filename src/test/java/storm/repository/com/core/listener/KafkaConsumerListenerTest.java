package storm.repository.com.core.listener;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class KafkaConsumerListenerTest {

    @Test
    void consumeMessage_delegatesToHandler() {
        MessageHandler handler = mock(MessageHandler.class);
        KafkaConsumerListener listener = new KafkaConsumerListener(handler);

        listener.consumeMessage("{\"test\":true}");

        verify(handler).processMessage("{\"test\":true}");
    }
}
