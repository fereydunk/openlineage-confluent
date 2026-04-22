package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumes from ol-raw-orders using a fixed consumer group so the bridge
 * detects it automatically via the Metrics API consumer_lag_offsets signal.
 *
 * No OpenLineage SDK calls are needed here — the bridge scrapes consumer group
 * identity from the Confluent Metrics API every poll cycle and emits the
 * lineage edge kafka-consumer-group://<cluster_id>/ol-java-demo-consumer.
 *
 * Required env vars (cluster-scoped Kafka API key, not a cloud key):
 *   KAFKA_BOOTSTRAP_SERVERS  e.g. pkc-xxx.us-west-2.aws.confluent.cloud:9092
 *   KAFKA_API_KEY
 *   KAFKA_API_SECRET
 *
 * Optional:
 *   OL_TOPIC              default ol-raw-orders
 *   POLL_DURATION_SECS    default 90  (long enough for the Metrics API to see the group)
 */
public class OrderConsumer {

    private static final String GROUP_ID = "ol-java-demo-consumer";

    public static void main(String[] args) {
        String bootstrap  = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String apiKey     = requireEnv("KAFKA_API_KEY");
        String apiSecret  = requireEnv("KAFKA_API_SECRET");
        String topic      = env("OL_TOPIC", "ol-raw-orders");
        long   pollSecs   = Long.parseLong(env("POLL_DURATION_SECS", "90"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ol-java-order-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";");

        System.out.println("Starting consumer  group.id=" + GROUP_ID + "  topic=" + topic);
        System.out.printf("Polling for %ds — Metrics API needs ~2 min to surface the group%n", pollSecs);

        long deadline = System.currentTimeMillis() + pollSecs * 1_000L;
        long received = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
                received += records.count();
                if (records.count() > 0)
                    System.out.printf("  consumed %d messages (total=%d)%n", records.count(), received);
            }
        }

        System.out.printf("%nDone. Consumed %d messages total.%n", received);
        System.out.println();
        System.out.println("Next: wait ~2 min then run 'make validate'");
        System.out.println("Look for '" + GROUP_ID + "' in the lineage edges table.");
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
