package org.vvcephei.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

public class Analyzer {
    private static final class MetaDeserializer implements Deserializer<Object> {

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public Object deserialize(final String topic, final byte[] bytes) {
            switch (topic) {
                case "input":
                    return new IntegerDeserializer().deserialize(topic, bytes);
                case "output":
                    return new StringDeserializer().deserialize(topic, bytes);
                default:
                    return Arrays.toString(bytes);
            }
        }

        @Override
        public void close() {

        }
    }

    public static void main(String[] args) {
        final String topic = "output";
        final Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, App.Rollup> consumer =
            new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new App.RollupSerde());
        consumer.subscribe(Collections.singleton(topic));

        final Map<String, List<ConsumerRecord<String, App.Rollup>>> sorted = new TreeMap<>();
        int attempts = 2;
        while (attempts > 0) {
            System.out.println("polling");
            final ConsumerRecords<String, App.Rollup> poll = consumer.poll(Duration.ofSeconds(1));
            if (poll.count() > 0) {
                attempts = 2;
            } else {
                attempts--;
            }
            for (final ConsumerRecord<String, App.Rollup> record : poll) {
                System.out.println(record);
                sorted.computeIfAbsent(record.key() + "_" + record.value().getWindowStart(), k -> new LinkedList<>())
                      .add(record);
            }
        }

        int results = 0;
        System.out.println("RESULTS");
        for (final Map.Entry<String, List<ConsumerRecord<String, App.Rollup>>> entry : sorted.entrySet()) {
            System.out.printf("%d\t%s\t%s%n", entry.getValue().size(), entry.getKey(), entry.getValue());
            results++;
        }

        int invalids = 0;
        System.out.println("INVALIDS");
        for (final Map.Entry<String, List<ConsumerRecord<String, App.Rollup>>> entry : sorted.entrySet()) {
            if (entry.getValue().size() > 1) {
                System.out.printf("%d\t%s%n", entry.getValue().size(), entry.getKey());
                for (final ConsumerRecord<String, App.Rollup> record : entry.getValue()) {
                    System.out.println("\t" + record);
                    pprintRecord(record);
                }
                invalids++;
            }
        }

        System.out.printf("results:%d invalids:%d%n", results, invalids);
    }

    public static void pprintRecord(final ConsumerRecord<?, ?> record) {
        if (record.headers() != null) {
            for (final Header header : record.headers()) {
                if (header.key().endsWith("provenance-offset")
                    || header.key().endsWith("provenance-stream-time")) {
                    System.out.println("\t\t" + header.key() + ": " + new LongDeserializer().deserialize(null, header.value()));
                } else if (header.key().endsWith("provenance-partition")) {
                    System.out.println("\t\t" + header.key() + ": " + new IntegerDeserializer().deserialize(null, header.value()));
                } else if (header.key().endsWith("provenance-key-string")
                    || header.key().endsWith("provenance-thread")
                    || header.key().endsWith("provenance-topic")) {
                    System.out.println("\t\t" + header.key() + ": " + new StringDeserializer().deserialize(null, header.value()));
                }
            }
        }
    }
}
