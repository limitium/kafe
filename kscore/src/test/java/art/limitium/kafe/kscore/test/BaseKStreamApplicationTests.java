package art.limitium.kafe.kscore.test;

import art.limitium.kafe.kscore.kstreamcore.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BaseKStreamApplicationTests {
    public static final int DEFAULT_READ_TIMEOUT_SECONDS = 20;
    public static String[] consumerTopics;

    public static class CustomExecutionListener implements TestExecutionListener {

        @Override
        public void beforeTestClass(TestContext testContext) {
            consumerTopics = Arrays.stream(getTestClassesHierarchy(testContext.getTestClass())
                            .stream()
                            .map(c -> c.getAnnotation(KafkaTest.class))
                            .filter(Objects::nonNull).findFirst()
                            .map(KafkaTest::consumers)
                            .orElse(new String[0]))
                    .filter(Predicate.not(String::isBlank))
                    .toArray(String[]::new);
        }

        public static List<Class<?>> getTestClassesHierarchy(Class<?> testClass) {
            List<Class<?>> classList = new ArrayList<>();
            Class<?> superclass = testClass.getSuperclass();
            classList.add(testClass);
            while (superclass != null) {
                classList.add(superclass);
                superclass = superclass.getSuperclass();
            }
            return classList;
        }
    }

    @DynamicPropertySource
    public static void testConsumerTopics(DynamicPropertyRegistry registry) {
        registry.add("test.consumer.topics", () -> consumerTopics);
        registry.add("spring.kafka.streams.cleanup.on-startup", () -> true);
        registry.add("spring.kafka.streams.cleanup.on-shutdown", () -> true);
    }

    @Configuration
    public static class BaseKafkaTestConfig {

        @Component
        public static class KafkaProducer {

            private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

            @Autowired
            private KafkaTemplate<byte[], byte[]> kafkaTemplate;

            public void send(String topic, Integer partition, byte[] key, byte[] value) {
                LOGGER.info("sending to topic='{}' payload='{}'", topic, value);
                kafkaTemplate.send(topic, partition, key, value);
            }

            public void send(ProducerRecord<byte[], byte[]> producerRecord) {
                LOGGER.info("sending to topic='{}' payload='{}'", producerRecord.topic(), producerRecord.value());
                kafkaTemplate.send(producerRecord);
            }
        }

        @Component
        public class KafkaConsumer {
            private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

            @Value("${test.consumer.topics}")
            private String[] consumerTopics;

            private Map<String, LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>>> received =
                    new HashMap<>();

            @PostConstruct
            public void fillReceived() {
                for (String consumerTopic : consumerTopics) {
                    LOGGER.info("Create receiver for topic " + consumerTopic);
                    received.put(consumerTopic, new LinkedBlockingQueue<>());
                }
            }

            @KafkaListener(topics = "#{'${test.consumer.topics}'.split(',')}", groupId = "consumer1")
            public void receive(ConsumerRecord<byte[], byte[]> consumerRecord) {
                LOGGER.info("received payload='{}'", consumerRecord.toString());
                received.get(consumerRecord.topic()).add(consumerRecord);
            }

            public ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic, int timeout) {
                assertTopic(topic);
                try {
                    ConsumerRecord<byte[], byte[]> record =
                            received.get(topic).poll(timeout, TimeUnit.SECONDS);
                    assertNotNull(record, "Topic `" + topic + "` is empty after " + timeout + "s");
                    return record;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            public List<ConsumerRecord<byte[], byte[]>> getAllRecordsFrom(String topic, int timeoutInMillis) {
                assertTopic(topic);
                try {
                    List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
                    long start = System.currentTimeMillis();
                    long end = start + timeoutInMillis;
                    while (System.currentTimeMillis() < end) {
                        results.add(received.get(topic).poll(timeoutInMillis, TimeUnit.MILLISECONDS));
                    }
                    return results;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            public void ensureEmpty(String topic) {
                assertTopic(topic);
                try {
                    ConsumerRecord<byte[], byte[]> mustBeEmpty =
                            received.get(topic).poll(100, TimeUnit.MILLISECONDS);
                    assertNull(mustBeEmpty, "Topic `" + topic + "` is not empty");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            public void clearTopic(String topic) {
                assertTopic(topic);
                LOGGER.info("Clearing topic + " + topic);
                received.get(topic).clear();
            }

            public void clearAllTopics() {
                for (String consumerTopic : consumerTopics) {
                    clearTopic(consumerTopic);
                }
            }

            private void assertTopic(String topic) {
                if (!received.containsKey(topic)) {
                    throw new RuntimeException(
                            "Topic `" + topic + "` wasn't set as consumer in @KafkaTest annotation");
                }
            }
        }
    }

    @Autowired
    private BaseKafkaTestConfig.KafkaConsumer consumer;

    @Autowired
    private BaseKafkaTestConfig.KafkaProducer producer;

    /**
     * Sends key and value in {@link Topic#topic}
     *
     * @param topic
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     */
    protected <K, V> void send(Topic<K, V> topic, K key, V value) {
        send(
                topic.topic,
                topic.keySerde.serializer().serialize(topic.topic, key),
                topic.valueSerde.serializer().serialize(topic.topic, value));
    }

    /**
     * Sends message into `topicName` instead of {@link Topic#topic}
     *
     * @param topic
     * @param topicName Overrides topic if it has `*` as a subscription pattern in {@link
     *                  Topic#topic}
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     */
    protected <K, V> void send(Topic<K, V> topic, String topicName, K key, V value) {
        send(
                topic,
                null,
                topicName,
                key,
                value);
    }

    protected <K, V> void send(Topic<K, V> topic, Integer partition, K key, V value) {
        send(
                topic,
                partition,
                topic.topic,
                key,
                value);
    }

    protected <K, V> void send(Topic<K, V> topic, Integer partition, String topicName, K key, V value) {
        send(
                topicName,
                partition,
                topic.keySerde.serializer().serialize(topicName, key),
                topic.valueSerde.serializer().serialize(topicName, value));
    }

    protected void send(String topic, byte[] value) {
        send(topic, null, null, value);
    }

    protected void send(String topic, byte[] key, byte[] value) {
        send(topic, null, key, value);
    }

    protected void send(String topic, Integer partition, byte[] key, byte[] value) {
        producer.send(topic, partition, key, value);
    }

    protected void send(ProducerRecord<byte[], byte[]> producerRecord) {
        producer.send(producerRecord);
    }

    /**
     * Wait for a single message in a concrete {@link Topic}
     *
     * @param topic   read topic with key and value serdes
     * @param timeout await timout
     * @param <K>     expected key type
     * @param <V>     expected value type
     * @return
     * @see BaseKStreamApplicationTests#waitForRecordFrom(Topic)
     */
    protected <K, V> ConsumerRecord<K, V> waitForRecordFrom(Topic<K, V> topic, int timeout) {
        ConsumerRecord<byte[], byte[]> record = waitForRecordFrom(topic.topic, timeout);

        Serde<K> keySerde = topic.keySerde;
        Serde<V> valueSerde = topic.valueSerde;
        return createConsumedRecord(record, keySerde, valueSerde);
    }

    protected <K, V> ConsumerRecord<K, V> waitForRecordFrom(Topic<K, V> topic) {
        return waitForRecordFrom(topic, DEFAULT_READ_TIMEOUT_SECONDS);
    }

    protected ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic) {
        return waitForRecordFrom(topic, DEFAULT_READ_TIMEOUT_SECONDS);
    }

    protected ConsumerRecord<byte[], byte[]> waitForRecordFrom(String topic, int timeout) {
        return consumer.waitForRecordFrom(topic, timeout);
    }

    protected <K, V> ConsumerRecord<K, V> waitForRecordFrom(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        return createConsumedRecord(waitForRecordFrom(topic, DEFAULT_READ_TIMEOUT_SECONDS), keySerde, valueSerde);
    }

    protected List<ConsumerRecord<byte[], byte[]>> getAllRecordsFrom(String topic, int timeoutInMillis) {
        return consumer.getAllRecordsFrom(topic, timeoutInMillis);
    }

    protected void clearAllTopics(){
        consumer.clearAllTopics();
    }

    @NotNull
    private <K, V> ConsumerRecord<K, V> createConsumedRecord(ConsumerRecord<byte[], byte[]> record, Serde<K> keySerde, Serde<V> valueSerde) {
        return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                record.key() != null ? record.key().length : 0,
                record.value() != null ? record.value().length : 0,
                keySerde.deserializer().deserialize(record.topic(), record.key()),
                valueSerde.deserializer().deserialize(record.topic(), record.value()),
                record.headers(),
                Optional.empty());
    }

    protected <K, V> void ensureEmptyTopic(Topic<K, V> topic) {
        consumer.ensureEmpty(topic.topic);
    }

    protected void ensureEmptyTopic(String topic) {
        consumer.ensureEmpty(topic);
    }
}
