package org.vvcephei.demo;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public final class App {
    private App() {}

    public static final class RollupSerde implements Serde<Rollup>, Serializer<Rollup>, Deserializer<Rollup> {

        @Override
        public Rollup deserialize(final String s, final byte[] bytes) {
            return Rollup.deserialize(ByteBuffer.wrap(bytes));
        }

        @Override
        public byte[] serialize(final String s, final Rollup rollup) {
            return rollup.serialize().array();
        }

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<Rollup> serializer() {
            return new RollupSerde();
        }

        @Override
        public Deserializer<Rollup> deserializer() {
            return new RollupSerde();
        }
    }

    public static final class Rollup {
        private final long windowStart;
        private final int count;
        private final int sum;
        private final int min;
        private final int max;
        private final int last;

        // "empty" rollup must always be merged as the left rollup
        private Rollup() {
            windowStart = -1;
            count = 0;
            sum = 0;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            last = -1;
        }

        // Rollup with unknown window
        private Rollup(final int count,
                       final int sum,
                       final int min,
                       final int max,
                       final int last) {
            this.windowStart = -1;
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
            this.last = last;
        }

        private Rollup(final long windowStart,
                       final int count,
                       final int sum,
                       final int min,
                       final int max,
                       final int last) {
            this.windowStart = windowStart;
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
            this.last = last;
        }

        private Rollup withWindow(final Window window) {
            return new Rollup(window.start(),
                              count,
                              sum,
                              min,
                              max,
                              last);
        }

        private static Rollup merge(final Rollup left, final Rollup right) {
            if (left.windowStart != -1 && left.windowStart != right.windowStart) {
                throw new IllegalArgumentException(left + " <> " + right);
            }
            return new Rollup(right.windowStart,
                              left.count + right.count,
                              left.sum + right.sum,
                              Math.min(left.min, right.min),
                              Math.max(left.max, right.max),
                              right.last);
        }

        public long getWindowStart() {
            return windowStart;
        }

        public int getCount() {
            return count;
        }

        public int getSum() {
            return sum;
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public int getLast() {
            return last;
        }

        ByteBuffer serialize() {
            return ByteBuffer.allocate(Long.BYTES + 5 * Integer.BYTES)
                             .putLong(windowStart)
                             .putInt(count)
                             .putInt(sum)
                             .putInt(min)
                             .putInt(max)
                             .putInt(last);
        }

        static Rollup deserialize(final ByteBuffer buffer) {
            return new Rollup(buffer.getLong(),
                              buffer.getInt(),
                              buffer.getInt(),
                              buffer.getInt(),
                              buffer.getInt(),
                              buffer.getInt()
            );
        }

        @Override
        public String toString() {
            return "Rollup{" +
                "windowStart=" + windowStart +
                ", count=" + count +
                ", sum=" + sum +
                ", min=" + min +
                ", max=" + max +
                ", last=" + last +
                '}';
        }
    }

    public static void main(final String[] args) {
        if (args[0].equals("clean")) {
            Producer.cleanTopics("output");
        }
        Producer.createTopics("output");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input", Consumed.with(Serdes.String(), Serdes.Integer()))
               .groupByKey()
               .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ofSeconds(1)))
               .aggregate(
                   Rollup::new,
                   (k, v, agg) -> Rollup.merge(agg, new Rollup(1, v, v, v, v)),
                   Materialized.<String, Rollup, WindowStore<Bytes, byte[]>>as("myagg_state").withValueSerde(new RollupSerde())
               )
               .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("myagg_suppress_state"))
               .mapValues(((key, rollup) -> rollup.withWindow(key.window())))
               .toStream(((stringWindowed, rollup) -> stringWindowed.key()))
               .to("output", Produced.with(Serdes.String(), new RollupSerde()));

        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "myagg_app");
        streamsConfiguration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/myagg_app");
        streamsConfiguration.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "30000");
//        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, com.booking.infra.rollup.kafka.TimestampedValueTimestampExtractor.class);
        streamsConfiguration.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();

        System.out.println("Hello World!");
    }
}
