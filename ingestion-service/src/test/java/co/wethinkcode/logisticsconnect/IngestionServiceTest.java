package co.wethinkcode.logisticsconnect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IngestionServiceTest {

    @Test
    @DisplayName("GET /hubs")
    public void shouldGetHubs() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        HttpResponse<String> response = Unirest.get("http://localhost:7050/hubs").asString();
        assertEquals(200, response.getStatus());
        ArrayNode intersections = objectMapper.readValue(response.getBody(), ArrayNode.class);

        for (JsonNode intersection : intersections) {
            assertTrue(intersection.asText().contains("hub_id"));
            assertTrue(intersection.asText().contains("Province"));
            assertTrue(intersection.asText().contains("Sorting_center"));
            assertTrue(intersection.asText().contains("active"));
        }

    }
}
