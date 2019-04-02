package org.vvcephei.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    private static final class MetaKeyDeserializer implements Deserializer<Object> {

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public Object deserialize(final String topic, final byte[] bytes) {
            switch (topic) {
                case "input":
                case "output":
                    return new StringDeserializer().deserialize(topic, bytes);
                case "myagg_app-myagg_suppress_state-store-changelog":
                    return WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer().deserialize(topic, bytes);
                default:
                    return Arrays.toString(bytes);
            }
        }

        @Override
        public void close() {

        }
    }

    private static final class MetaValueDeserializer implements Deserializer<Object> {

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public Object deserialize(final String topic, final byte[] bytes) {
            switch (topic) {
                case "input":
                    return new IntegerDeserializer().deserialize(topic, bytes);
                case "output":
                    return new App.RollupSerde().deserialize(topic, bytes);
                default:
                    return Arrays.toString(bytes);
            }
        }

        @Override
        public void close() {

        }
    }

    public static void main(String[] args) {
        final String topic = args[0];
        final Long offset;
        if (args.length > 1) {
            offset = Long.valueOf(args[1]);
        } else {
            offset = null;
        }
        final Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
//        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetaValueDeserializer.class.getCanonicalName());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig, new MetaKeyDeserializer(), new MetaValueDeserializer());
        consumer.subscribe(Collections.singleton(topic));
        while (true) {
            System.out.println("polling");
            final ConsumerRecords<Object, Object> poll = consumer.poll(Duration.ofSeconds(1));
            for (final ConsumerRecord<Object, Object> record : poll) {
                if (offset == null || offset.equals(record.offset())) {
                    System.out.println(record);
                    Analyzer.pprintRecord(record);
                }
            }
        }
    }
}
