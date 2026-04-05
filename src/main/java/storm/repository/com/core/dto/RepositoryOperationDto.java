package storm.repository.com.core.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RepositoryOperationDto {
    private String method;
    private String collection;
    private Map<String, Object> filter;
    private Object payload;
    private Integer limit;
    private String cursor;
    private Integer defaultLimit;
    private Integer maxLimit;
}
