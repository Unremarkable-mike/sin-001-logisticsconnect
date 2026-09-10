package co.wethinkcode.logisticsconnect;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.text.WordUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import io.javalin.Javalin;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IngestionServiceApp {

    private static String HUB_DATA;

    public static void main(String[] args) {

        Javalin app = Javalin.create().start(7050);
        ObjectMapper mapper = new ObjectMapper();

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO: read and clean src/main/resources/hubs-global.csv (hubs, sorting centers, regional districts data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.

        app.get("/hubs", ctx -> ctx.result(mapper.writeValueAsString(readAndCleanCSV())));
    }

    private static ArrayNode readAndCleanCSV() throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode hubData = mapper.createArrayNode();
        String text;
        String[] rows;

        try (InputStream is = IngestionServiceApp.class.getClassLoader().getResourceAsStream("hubs-global.csv")) {
            if (is == null) {throw new IllegalArgumentException("File not found!");}
            text = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        rows = text.split("\n");
        String[] row = rows[0].split(",");

        for (int i = 1; i < rows.length; i++) {
            Map<String, Object> data = new HashMap<>();

            for (int j = 0; j < row.length; j++) {
                String key = formatText(row[j]).toLowerCase();
                data.put(key , formatText(rows[i].split(",")[j]));
            }
            String jsonString = mapper.writeValueAsString(data);
            hubData.add(jsonString);
        }

        return hubData;
    }

    private static String formatText(String text) {
        text =  text.replace("\n", "").replace("\r", "");

        try {
            String[] words = text.split(" ");
            text = "";
            for (String word : words) {
                if (!word.isEmpty()) {
                    word = word.trim();
                    word = WordUtils.capitalize(word);

                    text += word + " ";
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        text = text.trim();

        switch (text.toLowerCase()) {
            case "true", "1", "yes" -> text = "Y";
            case "false", "0", "no" -> text = "N";
            case "unknown", "", "null", "-" -> text = "N/A";
        }

        return text;
    }
}

