package org.vvcephei.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public final class SessionWindowApp {
    private static final Logger log = LoggerFactory.getLogger(SessionWindowApp.class);
    private SessionWindowApp() {}

    public static void main(final String[] args) {
        Producer.createTopics("output");
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("input", Consumed.with(Serdes.String(), Serdes.Integer()))
            .mapValues(Object::toString)
            .peek((k, v) -> log.info("--> ping, k={},v={}", k, v))
            .peek((k, v) -> System.out.printf("[%d] --> ping, k=%s,v=%s%n", System.currentTimeMillis(), k, v))
            .transformValues(new ValueTransformerWithKeySupplier<String, String, String>() {
                @Override
                public ValueTransformerWithKey<String, String, String> get() {
                    return new ValueTransformerWithKey<String, String, String>() {
                        private ProcessorContext processorContext;

                        @Override
                        public void init(final ProcessorContext processorContext) {
                            this.processorContext = processorContext;
                        }

                        @Override
                        public String transform(final String key, final String value) {
                            log.info("--> ping, k={},v={},timestamp={}", key, value, processorContext.timestamp());
                            System.out.printf("[%d] --> ping, k=%s,v=%s,timestamp=%d%n", System.currentTimeMillis(), key, value, processorContext.timestamp());
                            return value;
                        }

                        @Override
                        public void close() {

                        }
                    };
                }
            })
            .groupBy((k, v) -> v, Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(SessionWindows.with(Duration.ofSeconds(100)).grace(Duration.ofMillis(20)))
            .count()
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .peek((k, v) -> log.info("window={},k={},v={}", k.window(), k.key(), v))
            .peek((k, v) -> System.out.printf("[%d] window=%s,k=%s,v=%s%n", System.currentTimeMillis(), k.window(), k.key(), v))
            .transformValues(new ValueTransformerWithKeySupplier<Windowed<String>, Long, Long>() {
                @Override
                public ValueTransformerWithKey<Windowed<String>, Long, Long> get() {
                    return new ValueTransformerWithKey<Windowed<String>, Long, Long>() {
                        private ProcessorContext processorContext;

                        @Override
                        public void init(final ProcessorContext processorContext) {
                            this.processorContext = processorContext;
                        }

                        @Override
                        public Long transform(final Windowed<String> key, final Long value) {
                            log.info("window={},k={},v={},timestamp={}", key.window(), key.key(), value, processorContext.timestamp());
                            System.out.printf("[%d] window=%s,k=%s,v=%s,timestamp=%d%n", System.currentTimeMillis(), key.window(), key.key(), value, processorContext.timestamp());
                            return value;
                        }

                        @Override
                        public void close() {

                        }
                    };
                }
            });


        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "sessions_app");
        streamsConfiguration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
