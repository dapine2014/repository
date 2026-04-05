package storm.repository.com.core.dto;

import lombok.*;

import java.util.List;

@Builder
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TopicDto {
    private String uuIdFrom;
    private String from;
    private String to;
    private String uuIdTo;
    private List<QueryDto> query;
    private List<Object> response;
}
