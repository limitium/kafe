package art.limitium.kafe.kscore;

import art.limitium.kafe.kscore.kstreamcore.KStreamInfraCustomizer;
import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.test.BaseKStreamApplicationTests;
import art.limitium.kafe.kscore.test.KafkaTest;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "core-app.in.topic.1",
                "core-app.store-kv-inject"
        },
        consumers = {
                "core-app.store-kv-changelog"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSInjectorProcessorTest.InjectTopologyConfig.class
        })
public class KSInjectorProcessorTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, Integer> SOURCE = new Topic<>("core-app.in.topic.1", Serdes.Integer(), Serdes.Integer());
    public static final Topic<Integer, Integer> INJECT = new Topic<>("core-app.store-kv-inject", Serdes.Integer(), Serdes.Integer());
    public static final Topic<Integer, Integer> CHANGELOG = new Topic<>("core-app.store-kv-changelog", Serdes.Integer(), Serdes.Integer());

    @Configuration
    public static class InjectTopologyConfig {
        public static class SumProcessor implements ExtendedProcessor<Integer, Integer, Integer, Integer> {

            private KeyValueStore<Integer, Integer> kv;

            @Override
            public void init(ExtendedProcessorContext<Integer, Integer, Integer, Integer> context) {
                kv = context.getStateStore("kv");
            }

            @Override
            public void process(Record<Integer, Integer> record) {
                Integer currentValue = kv.get(record.key());
                if (currentValue == null) {
                    currentValue = 0;
                }
                currentValue += record.value();
                kv.put(record.key(), currentValue);
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                StoreBuilder<KeyValueStore<Integer, Integer>> store = Stores2.keyValueStoreBuilder(Stores.persistentKeyValueStore("kv"), Serdes.Integer(), Serdes.Integer())
                        .addInjector();

                builder
                        .addProcessor(SumProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .done();
            };

        }
    }

    @BeforeEach
    void setup(){
        clearAllTopics();
    }

    @Test
    void testDirectInject() {
        send(SOURCE, 1, 2);
        ConsumerRecord<Integer, Integer> changed = waitForRecordFrom(CHANGELOG);

        assertEquals(1, changed.key());
        assertEquals(2, changed.value());

        send(INJECT, 1, 5);
        changed = waitForRecordFrom(CHANGELOG);

        assertEquals(1, changed.key());
        assertEquals(5, changed.value());

        send(SOURCE, 1, 1);
        changed = waitForRecordFrom(CHANGELOG);

        assertEquals(1, changed.key());
        assertEquals(6, changed.value());
    }
}

