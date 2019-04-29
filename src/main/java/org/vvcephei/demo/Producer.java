package org.vvcephei.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public final class Producer {

    private Producer() {}

    public static void main(final String[] args) throws ParseException {
        final Random keySequence = new Random(3); // is any number more random than 3? ;)
        final Random valueSequence = new Random(4); // is any number more random than 4? ;)

        final Duration produceDuration = Duration.parse(args[0]);
        final Duration producePeriod = Duration.parse(args[1]);
        final int numKeys = NumberFormat.getInstance().parse(args[2]).intValue();
        final int numValues = NumberFormat.getInstance().parse(args[3]).intValue();

        System.out.printf("producing %d keys and %d values, one every %s for %s%n", numKeys, numValues, producePeriod, produceDuration);
        final long start = System.currentTimeMillis();
        cleanTopics("input");
        createTopics("input");

        final Iterator<String> keys = new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                return "key" + keySequence.nextInt(numKeys);
            }
        };

        final Iterator<Integer> values = new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                return valueSequence.nextInt(numValues);
            }
        };

        produceFor("input", keys, values, produceDuration, producePeriod);

        System.out.println("done in " + Duration.ofMillis(System.currentTimeMillis() - start));
    }

    private static void produceFor(final String topic, final Iterator<String> keys, final Iterator<Integer> values, final Duration duration, final Duration producePeriod) {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getCanonicalName());


        final NumberFormat numberFormat = NumberFormat.getInstance();
        final long start = System.nanoTime();
        final long limit = duration.toNanos();
        try (final org.apache.kafka.clients.producer.Producer<String, Integer> producer = new KafkaProducer<>(properties)) {
            int i = 1;
            long spent;
            long last = 0;
            while (limit > (spent = System.nanoTime() - start)) {
                final String key = keys.next();
                producer.send(new ProducerRecord<>(topic, key, values.next()));
                producer.flush();
                if (i % 10_000_000 == 0 || (System.nanoTime() - last) > 1_000_000_000) {
                    System.out.println("produced " + numberFormat.format(i) + " to " + topic + " in " + Duration.ofNanos(spent));
                    last = System.nanoTime();
                }
                Thread.sleep(producePeriod.toMillis());
                i++;
            }
            System.out.println("produced " + numberFormat.format(i) + " to " + topic + " in " + Duration.ofNanos(System.nanoTime() - start));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void cleanTopics(final String... topics) {
        final Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (final AdminClient admin = AdminClient.create(properties)) {

            Set<String> toDelete;
            do {
                try {
                    toDelete = intersection(new HashSet<>(asList(topics)), admin.listTopics().names().get());
                } catch (final ExecutionException e) {
                    throw new RuntimeException(e);
                }
                try {
                    admin.deleteTopics(toDelete).all().get();
                } catch (final ExecutionException e) {
                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw new RuntimeException(e);
                    }
                }
                System.out.println("deleted " + toDelete);
            } while (!toDelete.isEmpty());
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> intersection(final Set<String> left, final Set<String> right) {
        final Set<String> result = new HashSet<>();
        for (final String l : left) {
            if (right.contains(l)) {
                result.add(l);
            }
        }
        return result;
    }

    public static void createTopics(final String... topics) {
        final Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (final AdminClient admin = AdminClient.create(properties)) {
            Thread.sleep(10000);


            final Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
            topicConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Long.toString(Long.MAX_VALUE));
            topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(Long.MAX_VALUE));
            topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, Long.toString(Long.MAX_VALUE));
            topicConfig.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, Long.toString(Long.MAX_VALUE));

            try {
                admin.createTopics(
                    stream(topics).map(t -> {
                        final NewTopic newTopic = new NewTopic(t, 2, (short) 1);
                        newTopic.configs(topicConfig);
                        return newTopic;
                    }).collect(toList())
                ).all().get();
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("ignoring: " + e.getMessage());
                } else {
                    throw new RuntimeException(e);
                }
            }

            try {
                final Map<String, TopicDescription> descriptions = admin.describeTopics(asList(topics)).all().get();
                System.out.println("created " + descriptions);
            } catch (final ExecutionException e) {
                throw new RuntimeException(e);
            }
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
