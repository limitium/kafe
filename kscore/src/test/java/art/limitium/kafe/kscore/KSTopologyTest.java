package art.limitium.kafe.kscore;

import art.limitium.kafe.kscore.kstreamcore.Broadcast;
import art.limitium.kafe.kscore.kstreamcore.KStreamInfraCustomizer;
import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.test.BaseKStreamApplicationTests;
import art.limitium.kafe.kscore.test.KafkaTest;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQTransformer;
import art.limitium.kafe.kscore.kstreamcore.processor.DLQContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@KafkaTest(
        topics = {"test.in.topic.1"},
        consumers = {
                "test.out.topic.1",
                "test.out.topic.2",
                "test.out.broadcast.1",
                "test.out.dlq.1"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSTopologyTest.TopologyConfig.class
        })
class KSTopologyTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, Long> SOURCE = new Topic<>("test.in.topic.1", Serdes.Integer(), Serdes.Long());
    public static final Topic<String, String> SINK1 = new Topic<>("test.out.topic.1", Serdes.String(), Serdes.String());
    public static final Topic<String, String> SINK2 = new Topic<>("test.out.topic.2", Serdes.String(), Serdes.String());
    public static final Topic<Integer, String> BROADCAST_TOPIC = new Topic<>("test.out.broadcast.1", Serdes.Integer(), Serdes.String());
    public static final Broadcast<String> BROADCAST = new Broadcast<>(BROADCAST_TOPIC);
    public static final Topic<Integer, String> DLQ = new Topic<>("test.out.dlq.1", Serdes.Integer(), Serdes.String());
    public static final int BORADCAST_KEY_TRIGGER = -13;
    public static final int DLQ_CONTEXT_TEST_KEY = -99;

    @Configuration
    public static class TopologyConfig {
        public static class TestProcessor implements ExtendedProcessor<Integer, Long, String, String> {

            private KeyValueStore<Integer, Long> inMemKv;
            private ExtendedProcessorContext<Integer, Long, String, String> context;

            @Override
            public void init(ExtendedProcessorContext<Integer, Long, String, String> context) {
                this.context = context;
                inMemKv = context.getStateStore("in_mem_kv");
            }

            @Override
            public void process(Record<Integer, Long> record) {
                if (record.key() == BORADCAST_KEY_TRIGGER) {
                    context.broadcast(BROADCAST, "B13");
                    return;
                }
                if (record.key() == DLQ_CONTEXT_TEST_KEY) {
                    // Create and use custom DLQContext with hardcoded values
                    CustomDLQContext customDLQContext = new CustomDLQContext();
                    context.sendToDLQ(record, "DLQ context test", new RuntimeException("DLQ context test"), customDLQContext);
                    return;
                }
                if (record.value() < 0) {
                    context.sendToDLQ(record, new RuntimeException("negative"));
                    return;
                }

                inMemKv.putIfAbsent(record.key(), 0L);
                inMemKv.put(record.key(), inMemKv.get(record.key()) + record.value());

                context.send(record.value() % 2 != 0 ? SINK1 : SINK2, record
                        .withKey("k_" + record.key())
                        .withValue("v_" + inMemKv.get(record.key()))
                );
            }
        }

        /**
         * Custom DLQContext implementation for testing with hardcoded values
         */
        public static class CustomDLQContext implements DLQContext {
            private long customSequence = 1000L; // Start with custom sequence
            
            @Override
            public long getNextSequence() {
                return customSequence++; // Use custom sequence
            }
            
            @Override
            public String getTopic() {
                return "CUSTOM_TEST_TOPIC"; // Hardcoded custom topic
            }
            
            @Override
            public long getOffset() {
                return 9999L; // Hardcoded custom offset
            }
            
            @Override
            public int getPartition() {
                return 42; // Hardcoded custom partition
            }
            
            @Override
            public long currentLocalTimeMs() {
                return 1234567890000L; // Hardcoded custom timestamp
            }
        }


        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                StoreBuilder<KeyValueStore<Integer, Long>> store = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("in_mem_kv"), Serdes.Integer(), Serdes.Long());

                builder
                        .addProcessor(TestProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .withSink(SINK1)
                        .withSink(SINK2)
                        .withBroadcast(BROADCAST)
                        .withDLQ(DLQ, (failed, dlqContext, errorMessage, exception) -> failed.withValue(errorMessage))
                        .done();
            };

        }
    }

    @Test
    void testTopology() {
        send(SOURCE, 1, 1L);
        ConsumerRecord<String, String> out = waitForRecordFrom(SINK1);

        assertEquals("k_1", out.key());
        assertEquals("v_1", out.value());

        send(SOURCE, 1, 2L);
        out = waitForRecordFrom(SINK2);

        assertEquals("k_1", out.key());
        assertEquals("v_3", out.value());
    }

    @Test
    void testDQL() {
        send(SOURCE, 1, -1L);
        ConsumerRecord<Integer, String> out = waitForRecordFrom(DLQ);

        assertEquals(1, out.key());
        assertEquals("negative", out.value());
    }

    @Test
    void testBroadcast() {
        send(SOURCE, BORADCAST_KEY_TRIGGER, 777L);
        ConsumerRecord<Integer, String> b1 = waitForRecordFrom(BROADCAST_TOPIC);
        ConsumerRecord<Integer, String> b2 = waitForRecordFrom(BROADCAST_TOPIC);
        assertNotEquals(b1.partition(), b2.partition());
    }

    @Test
    void testDLQContext() {
        // Send a message that will trigger DLQ with custom context
        send(SOURCE, DLQ_CONTEXT_TEST_KEY, 123L);
        ConsumerRecord<Integer, String> dlqRecord = waitForRecordFrom(DLQ);

        // Verify the DLQ record contains the original key
        assertEquals(DLQ_CONTEXT_TEST_KEY, dlqRecord.key());
        
        // Verify the DLQ record value contains the error message (from lambda transformer)
        assertEquals("DLQ context test", dlqRecord.value());
        
        // Note: The custom DLQContext is passed to sendToDLQ but the lambda transformer
        // doesn't use it - it just returns the error message. This test verifies that
        // the custom DLQContext can be passed to sendToDLQ without errors.
    }
}
