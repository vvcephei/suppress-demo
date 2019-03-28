package org.vvcephei.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
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
        final Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
//        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetaDeserializer.class.getCanonicalName());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new MetaDeserializer());
        consumer.subscribe(Collections.singleton(topic));
        while (true) {
            System.out.println("polling");
            final ConsumerRecords<String, Object> poll = consumer.poll(Duration.ofSeconds(1));
            for (final ConsumerRecord<String, Object> record : poll) {
                System.out.println(record);
            }
        }
    }
}
