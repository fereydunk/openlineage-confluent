package demo;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpTransport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Produces order messages to ol-raw-orders and emits OpenLineage START/COMPLETE
 * events directly to Marquez — the only way to capture Kafka producer identity,
 * since the Confluent Metrics API exposes no client_id dimension for producers.
 *
 * Required env vars (cluster-scoped Kafka API key, not a cloud key):
 *   KAFKA_BOOTSTRAP_SERVERS  e.g. pkc-xxx.us-west-2.aws.confluent.cloud:9092
 *   KAFKA_API_KEY
 *   KAFKA_API_SECRET
 *
 * Optional:
 *   MARQUEZ_URL     default http://localhost:5000
 *   OL_TOPIC        default ol-raw-orders
 *   MESSAGE_COUNT   default 100
 */
public class OrderProducer {

    private static final String JOB_NAMESPACE = "java-app://order-service";
    private static final String JOB_NAME      = "order-producer";

    public static void main(String[] args) throws Exception {
        String bootstrap    = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String apiKey       = requireEnv("KAFKA_API_KEY");
        String apiSecret    = requireEnv("KAFKA_API_SECRET");
        String marquezUrl   = env("MARQUEZ_URL", "http://localhost:5000");
        String topic        = env("OL_TOPIC", "ol-raw-orders");
        int    messageCount = Integer.parseInt(env("MESSAGE_COUNT", "100"));

        String datasetNamespace = "kafka://" + bootstrap;

        // ── OpenLineage client ────────────────────────────────────────────────
        HttpTransport transport = HttpTransport.builder()
                .uri(marquezUrl)
                .build();
        OpenLineageClient olClient = new OpenLineageClient(transport);
        OpenLineage ol = new OpenLineage(
                URI.create("https://github.com/fereydunk/openlineage-confluent"));

        UUID runId = UUID.randomUUID();
        List<OpenLineage.OutputDataset> outputs = List.of(
                ol.newOutputDatasetBuilder()
                        .namespace(datasetNamespace)
                        .name(topic)
                        .build());

        // Emit START before touching Kafka
        olClient.emit(ol.newRunEventBuilder()
                .eventTime(ZonedDateTime.now())
                .eventType(OpenLineage.RunEvent.EventType.START)
                .job(ol.newJobBuilder().namespace(JOB_NAMESPACE).name(JOB_NAME).build())
                .run(ol.newRunBuilder().runId(runId).build())
                .outputs(outputs)
                .build());
        System.out.printf("[OL] START emitted  runId=%s  job=%s/%s%n",
                runId, JOB_NAMESPACE, JOB_NAME);

        // ── Kafka producer ────────────────────────────────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ol-java-order-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String orderId = "java-order-" + UUID.randomUUID().toString().substring(0, 8);
                String value = String.format(
                        "{\"order_id\":\"%s\",\"customer_id\":\"cust-%d\"," +
                        "\"amount\":%.2f,\"status\":\"PENDING\",\"source\":\"java-producer\"}",
                        orderId, i % 200, 9.99 + (i * 4.17));
                producer.send(new ProducerRecord<>(topic, orderId, value)).get();
                if ((i + 1) % 20 == 0)
                    System.out.printf("Produced %d/%d messages%n", i + 1, messageCount);
            }
        }
        System.out.printf("Done. Produced %d messages → %s%n", messageCount, topic);

        // Emit COMPLETE
        olClient.emit(ol.newRunEventBuilder()
                .eventTime(ZonedDateTime.now())
                .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                .job(ol.newJobBuilder().namespace(JOB_NAMESPACE).name(JOB_NAME).build())
                .run(ol.newRunBuilder().runId(runId).build())
                .outputs(outputs)
                .build());
        System.out.println("[OL] COMPLETE emitted");
        System.out.println();
        System.out.println("Check Marquez UI → http://localhost:3000");
        System.out.println("  namespace : " + JOB_NAMESPACE);
        System.out.println("  job       : " + JOB_NAME);
        System.out.println("  output    : " + datasetNamespace + "/" + topic);
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank())
            throw new IllegalStateException("Missing required env var: " + name);
        return v;
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
