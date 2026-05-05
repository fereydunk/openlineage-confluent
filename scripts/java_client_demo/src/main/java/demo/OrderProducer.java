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
 * Produces order messages to a Kafka topic and emits OpenLineage START/COMPLETE
 * events directly to Marquez — the only way to capture Kafka producer identity,
 * since the Confluent Metrics API exposes no client_id dimension for producers.
 *
 * Required env vars (cluster-scoped Kafka API key, not a cloud key):
 *   KAFKA_BOOTSTRAP_SERVERS  e.g. pkc-xxx.us-west-2.aws.confluent.cloud:9092
 *   KAFKA_API_KEY
 *   KAFKA_API_SECRET
 *
 * Optional:
 *   MARQUEZ_URL          default http://localhost:5000
 *   OL_TOPIC             default ol-raw-orders
 *   OL_JOB_NAMESPACE     default kafka-producer://order-service
 *   OL_JOB_NAME          default order-producer
 *   KAFKA_CLIENT_ID      default ol-java-order-producer
 *   MESSAGE_COUNT        default 100   (use -1 for infinite — long-running demo)
 *   MESSAGE_INTERVAL_MS  default 0     (sleep between sends; 1000 = 1 msg/s)
 */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        String bootstrap     = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String apiKey        = requireEnv("KAFKA_API_KEY");
        String apiSecret     = requireEnv("KAFKA_API_SECRET");
        String marquezUrl    = env("MARQUEZ_URL", "http://localhost:5000");
        String topic         = env("OL_TOPIC", "ol-raw-orders");
        String jobNamespace  = env("OL_JOB_NAMESPACE", "kafka-producer://order-service");
        String jobName       = env("OL_JOB_NAME", "order-producer");
        String clientId      = env("KAFKA_CLIENT_ID", "ol-java-order-producer");
        int    messageCount  = Integer.parseInt(env("MESSAGE_COUNT", "100"));
        long   intervalMs    = Long.parseLong(env("MESSAGE_INTERVAL_MS", "0"));
        boolean infinite     = (messageCount < 0);

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

        // JobTypeJobFacet makes the job recognisable in Marquez as a Kafka producer
        OpenLineage.JobFacets jobFacets = ol.newJobFacetsBuilder()
                .jobType(ol.newJobTypeJobFacetBuilder()
                        .processingType("STREAMING")
                        .integration("KAFKA")
                        .jobType("PRODUCER")
                        .build())
                .build();
        OpenLineage.Job job = ol.newJobBuilder()
                .namespace(jobNamespace)
                .name(jobName)
                .facets(jobFacets)
                .build();

        // Emit START before touching Kafka
        olClient.emit(ol.newRunEventBuilder()
                .eventTime(ZonedDateTime.now())
                .eventType(OpenLineage.RunEvent.EventType.START)
                .job(job)
                .run(ol.newRunBuilder().runId(runId).build())
                .outputs(outputs)
                .build());
        System.out.printf("[OL] START emitted  runId=%s  job=%s/%s%n",
                runId, jobNamespace, jobName);

        // ── Kafka producer ────────────────────────────────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";");

        // Emit COMPLETE on JVM shutdown so long-running producers (MESSAGE_COUNT=-1)
        // still flush a terminal lineage event when SIGTERM'd by the wizard teardown.
        UUID finalRunId = runId;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                olClient.emit(ol.newRunEventBuilder()
                        .eventTime(ZonedDateTime.now())
                        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                        .job(job)
                        .run(ol.newRunBuilder().runId(finalRunId).build())
                        .outputs(outputs)
                        .build());
                System.out.println("[OL] COMPLETE emitted (shutdown)");
            } catch (Exception ignored) { /* nothing useful to do at shutdown */ }
        }));

        long produced = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (infinite || produced < messageCount) {
                String orderId = "java-order-" + UUID.randomUUID().toString().substring(0, 8);
                String value = String.format(
                        "{\"order_id\":\"%s\",\"customer_id\":\"cust-%d\"," +
                        "\"amount\":%.2f,\"status\":\"PENDING\",\"source\":\"%s\"}",
                        orderId, (int)(produced % 200), 9.99 + (produced * 4.17), clientId);
                producer.send(new ProducerRecord<>(topic, orderId, value)).get();
                produced++;
                if (produced % 20 == 0) {
                    System.out.printf("Produced %d%s messages%n",
                            produced, infinite ? "" : "/" + messageCount);
                }
                if (intervalMs > 0) Thread.sleep(intervalMs);
            }
        }
        System.out.printf("Done. Produced %d messages → %s%n", produced, topic);

        // For finite runs, emit COMPLETE here (the shutdown hook is a backstop
        // for SIGTERM during infinite runs).
        if (!infinite) {
            olClient.emit(ol.newRunEventBuilder()
                    .eventTime(ZonedDateTime.now())
                    .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
                    .job(job)
                    .run(ol.newRunBuilder().runId(runId).build())
                    .outputs(outputs)
                    .build());
            System.out.println("[OL] COMPLETE emitted");
        }
        System.out.println();
        System.out.println("Check Marquez UI → http://localhost:3000");
        System.out.println("  namespace : " + jobNamespace);
        System.out.println("  job       : " + jobName);
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
