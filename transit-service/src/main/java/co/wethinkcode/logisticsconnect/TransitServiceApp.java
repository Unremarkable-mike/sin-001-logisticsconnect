package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TransitServiceApp {

    private static Connection connection;
    private static final String hubServiceAPI = "localhost:7051";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static JsonNode data;
    private static ArrayNode allHubs;

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
        connection = factory.createConnection();

        String brokerUrl = MqConfig.BROKER_URL;
        String topic = MqConfig.TOPIC;

        // Try-with-resources auto-invokes manager.close() when done
        try {
            ActiveMQTopicManager manager = new ActiveMQTopicManager(brokerUrl, topic);
            manager.get_messages();
            // Subscribe asynchronously using a lambda expression
            manager.subscribeAsync(msg -> System.out.println("[App 1 Received] -> " + msg));
            manager.subscribeAsync(msg -> System.out.println("[App 2 Received] -> " + msg.toUpperCase()));

            System.out.println("Subscribers are live. Waiting for messages...");

        } catch (Exception e) {
            e.printStackTrace();
        }

        Javalin app = Javalin.create().start(7053);
        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Calculates estimated arrival windows based on hub and delay stage.)
        // Add domain endpoints for transit-service here.

        app.get("/transit-service/{hub_id}", ctx -> {
            //finalMessage = "message";
            String finalMessage = getHubData(ctx.pathParam("hub_id"));

            if (!finalMessage.isEmpty()) {
                ctx.status(200).result("Received message: " + finalMessage);
            } else {
                ctx.status(404).result("No messages available in the queue.");
            }
        });
    }

    private static String dataForHub(JsonNode hubData,String hub_id) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String,String> map = new HashMap<>();

        if (hubData == null) {
            return "is empty";
        }
        map.put("hub_id", hub_id);

        return mapper.writeValueAsString(map);
    }


    private static String getHubData(String hub_id) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(hubServiceAPI+"/"+hub_id))
                .GET()
                .build();
        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString()
        );
        data = mapper.readTree(response.body());
        return dataForHub(data,hub_id);
    }
}

// MQ TODO: subscribes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)

class ActiveMQTopicManager implements AutoCloseable {
    private final Connection connection;
    private final Session session;
    private final Topic topic;

    // Constructor initializes the connection, session, and topic destination
    public ActiveMQTopicManager(String brokerUrl, String topicName) throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        this.connection = factory.createConnection();
        // Start connection automatically so consumer works instantly
        this.connection.setClientID("transit-service");
        this.connection.start();

        this.session = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.topic = this.session.createTopic(topicName);
    }

    // Method to register an asynchronous listener using a clean Java Consumer functional interface
    public void subscribeAsync(Consumer<String> messageProcessor) throws JMSException {
        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    String text = ((TextMessage) message).getText();
                    messageProcessor.accept(text); // Pass text payload to consumer callback
                }
            } catch (JMSException e) {
                System.err.println("Error parsing JMS text message: " + e.getMessage());
            }
        });

    }

    public void get_messages() throws JMSException {
        MessageConsumer consumer = session.createDurableConsumer(topic, "transit-service");

        while (true) {
            Message message = consumer.receiveNoWait();
            if (message == null) break;
            System.out.println("Received: "+((TextMessage) message).getText());
        }
    }

    // AutoCloseable implementation ensures safe connection tear-down
    @Override
    public void close() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
        System.out.println("ActiveMQ connection and session closed cleanly.");
    }
}