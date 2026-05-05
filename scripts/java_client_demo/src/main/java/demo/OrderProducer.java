package demo;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpTransport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
 * Produces Avro-encoded order messages to a Kafka topic and emits OpenLineage
 * START/COMPLETE events directly to Marquez.
 *
 * Why Avro: the provisioner pre-registers an AVRO schema for every topic so
 * downstream Flink SQL can read the value as a structured row. A plain string
 * payload fails Flink's deserializer with "Failed to deserialize Avro record"
 * and the whole pipeline dies. The schema this producer writes mirrors
 * RAW_ORDERS_SCHEMA in scripts/provision_demo_pipelines.py — keep them in sync.
 *
 * Required env vars:
 *   KAFKA_BOOTSTRAP_SERVERS  pkc-xxx.<region>.<cloud>.confluent.cloud:9092
 *   KAFKA_API_KEY            cluster-scoped Kafka API key
 *   KAFKA_API_SECRET
 *   SR_URL                   https://psrc-xxx.<region>.<cloud>.confluent.cloud
 *   SR_API_KEY               Schema-Registry-scoped API key
 *   SR_API_SECRET
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

    // Mirrors RAW_ORDERS_SCHEMA in scripts/provision_demo_pipelines.py.
    // MUST match RAW_ORDERS_SCHEMA in scripts/provision_demo_pipelines.py
    // AND the kafka-connect-datagen ORDERS quickstart schema
    // (mainline/datagen/src/main/resources/orders_schema.avro). Critical:
    //   - record name MUST be "orders" (lowercase plural, NOT "Order"
    //     singular) — Datagen uses lowercase, and SR rejects subjects with
    //     schemas of different fully-qualified names as "incompatible".
    //   - namespace MUST be "ksql" — same reason.
    // Mismatch here means "Schema being registered is incompatible with an
    // earlier schema for the subject" when Datagen and this producer both
    // try to register a schema for the same topic subject.
    private static final String VALUE_SCHEMA_JSON = """
        {
          "type": "record",
          "name": "orders",
          "namespace": "ksql",
          "fields": [
            {"name": "ordertime",  "type": "long"},
            {"name": "orderid",    "type": "int"},
            {"name": "itemid",     "type": "string"},
            {"name": "orderunits", "type": "double"},
            {"name": "address",    "type": {
                "type": "record", "name": "address",
                "fields": [
                    {"name": "city",    "type": "string"},
                    {"name": "state",   "type": "string"},
                    {"name": "zipcode", "type": "long"}
                ]}}
          ]
        }
        """;

    public static void main(String[] args) throws Exception {
        String bootstrap     = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String apiKey        = requireEnv("KAFKA_API_KEY");
        String apiSecret     = requireEnv("KAFKA_API_SECRET");
        String srUrl         = requireEnv("SR_URL");
        String srKey         = requireEnv("SR_API_KEY");
        String srSecret      = requireEnv("SR_API_SECRET");
        String marquezUrl    = env("MARQUEZ_URL", "http://localhost:5000");
        String topic         = env("OL_TOPIC", "ol-raw-orders");
        String jobNamespace  = env("OL_JOB_NAMESPACE", "kafka-producer://order-service");
        String jobName       = env("OL_JOB_NAME", "order-producer");
        String clientId      = env("KAFKA_CLIENT_ID", "ol-java-order-producer");
        int    messageCount  = Integer.parseInt(env("MESSAGE_COUNT", "100"));
        long   intervalMs    = Long.parseLong(env("MESSAGE_INTERVAL_MS", "0"));
        boolean infinite     = (messageCount < 0);

        Schema valueSchema   = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
        Schema addressSchema = valueSchema.getField("address").schema();
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

        olClient.emit(ol.newRunEventBuilder()
                .eventTime(ZonedDateTime.now())
                .eventType(OpenLineage.RunEvent.EventType.START)
                .job(job)
                .run(ol.newRunBuilder().runId(runId).build())
                .outputs(outputs)
                .build());
        System.out.printf("[OL] START emitted  runId=%s  job=%s/%s%n",
                runId, jobNamespace, jobName);

        // ── Kafka producer (Avro values, String keys) ────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";");
        // Schema Registry — required by KafkaAvroSerializer to register/look up
        // the writer schema. Auth follows SR's own basic-auth scheme (NOT the
        // cluster Kafka API key, NOT the cloud key — SR has its own key scope).
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srKey + ":" + srSecret);
        // Don't auto-register a (potentially conflicting) schema — assume the
        // provisioner already registered the target schema for this topic.
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);

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
        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (infinite || produced < messageCount) {
                String orderId = "java-order-" + UUID.randomUUID().toString().substring(0, 8);
                GenericRecord address = new GenericData.Record(addressSchema);
                address.put("city",    "city-" + (int)(produced % 50));
                address.put("state",   "ST" + (int)(produced % 50));
                address.put("zipcode", 90000L + (produced % 1000));

                GenericRecord order = new GenericData.Record(valueSchema);
                order.put("ordertime",  System.currentTimeMillis());
                order.put("orderid",    (int)(produced % Integer.MAX_VALUE));
                order.put("itemid",     "item-" + (int)(produced % 100));
                order.put("orderunits", 1.0 + (produced % 20));
                order.put("address",    address);

                producer.send(new ProducerRecord<>(topic, orderId, order)).get();
                produced++;
                if (produced % 20 == 0) {
                    System.out.printf("Produced %d%s messages%n",
                            produced, infinite ? "" : "/" + messageCount);
                }
                if (intervalMs > 0) Thread.sleep(intervalMs);
            }
        }
        System.out.printf("Done. Produced %d messages → %s%n", produced, topic);

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
