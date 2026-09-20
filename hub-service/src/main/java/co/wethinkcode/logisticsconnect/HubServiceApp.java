package co.wethinkcode.logisticsconnect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.javalin.Javalin;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class HubServiceApp {

    private static final String ingestionAPI = "http://localhost:7050/hubs";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static JsonNode data;

    public static void main(String[] args) throws JsonProcessingException {
        Javalin app = Javalin.create().start(7051);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/hubs/{hub_id}", ctx -> {
            data = mapper.readValue(getHubData(), ArrayNode.class);
            String namePlaceData = dataForSingleHub(ctx.pathParam("hub_id"));
            if (namePlaceData != null) {
                ctx.status(200).result(namePlaceData);
            }
            else ctx.status(404).result("Hub not found");
        });
    }


    private static String dataForSingleHub(String hub_id) throws JsonProcessingException {

        for  (int x = 0; x < data.size(); x++) {

            Map<String, Object> map = new HashMap<>();
            JsonNode hub = mapper.readTree(data.get(x).asText());
            String hubId = hub.get("hub_id").asText();

            if  (hubId.equals(hub_id)) {
                map.put("province", hub.get("province"));
                map.put("sorting_center", hub.get("sorting_center"));
                return mapper.writeValueAsString(map);
            }
        }

        return "";
    }

    private static String getHubData() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ingestionAPI))
                .GET()
                .build();
        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString()
        );

        return response.body();
    }
}
