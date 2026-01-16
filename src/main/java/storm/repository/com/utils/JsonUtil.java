package storm.repository.com.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(Object object){
        try{

            return objectMapper.writeValueAsString(object);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz){
        try {

            return objectMapper.readValue(json,clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
