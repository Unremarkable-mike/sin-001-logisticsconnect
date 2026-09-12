package co.wethinkcode.logisticsconnect;

import co.wethinkcode.logisticsconnect.mq.MqConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

public class DelayStageServiceApp {

    public static void main(String[] args) throws JMSException {

        Javalin app = Javalin.create().start(7052);

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/delay-stage/{hub_id}", DelayStageServiceApp::hub_stage);
        // TODO (Tracks the Transit Delay Stage (0-8, e.g. weather shutdowns).)
        // Add domain endpoints for delay-stage-service here.
        app.post("/delay-stage/{hub_id}", DelayStageServiceApp::publishHubData);
    }

    private static void hub_stage(Context ctx) {}

    private static void publishHubData(Context ctx) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode message = mapper.readTree(ctx.body());
        Map<String, String> map = new HashMap<>();

        map.put("hub_id", ctx.pathParam("hub_id"));
        map.put("stage", message.get("stage").asText());

        publishToActiveMQ.publish(mapper.writeValueAsString(map));

        if (ctx.pathParam("hub_id").contains("H-")) {
            ctx.status(200).result("Sent message: " + mapper.writeValueAsString(map));
        } else {
            ctx.status(404).result("Invalid hub id");
        }
    }
}


// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.logisticsconnect.mq.MqConfig)
class publishToActiveMQ{

    public static void publish(String m) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
        try
        {
            Connection connection = factory.createConnection();
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(MqConfig.TOPIC);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage message = session.createTextMessage();
            message.setText(m);
            System.out.println(m);
            producer.send(message);
            producer.close();
            connection.close();

        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}