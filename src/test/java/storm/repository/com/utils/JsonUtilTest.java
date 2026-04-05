package storm.repository.com.utils;

import org.junit.jupiter.api.Test;
import storm.repository.com.core.dto.MessageType;
import storm.repository.com.core.dto.RepositoryMessageDto;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilTest {

    @Test
    void toJson_andFromJson_roundTrip() {
        RepositoryMessageDto message = RepositoryMessageDto.builder()
                .type(MessageType.REQUEST)
                .requestId("req-1")
                .from("service-a")
                .to("repository")
                .build();

        String json = JsonUtil.toJson(message);
        RepositoryMessageDto parsed = JsonUtil.fromJson(json, RepositoryMessageDto.class);

        assertEquals("req-1", parsed.getRequestId());
        assertEquals(MessageType.REQUEST, parsed.getType());
        assertEquals("service-a", parsed.getFrom());
    }

    @Test
    void fromJson_invalidThrowsRuntimeException() {
        assertThrows(RuntimeException.class, () -> JsonUtil.fromJson("{invalid}", RepositoryMessageDto.class));
    }
}
