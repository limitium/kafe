package art.limitium.kafe.kscore;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.KSTopology;
import art.limitium.kafe.kscore.kstreamcore.KStreamInfraCustomizer;
import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.downstream.DownstreamDefinition;
import art.limitium.kafe.kscore.kstreamcore.downstream.converter.NewCancelConverter;
import art.limitium.kafe.kscore.kstreamcore.downstream.converter.CorrelationIdGenerator;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.ksmodel.downstream.Request;
import art.limitium.kafe.kscore.test.BaseKStreamApplicationTests;
import art.limitium.kafe.kscore.test.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;

import static art.limitium.kafe.kscore.kstreamcore.KStreamConfig.TolerableLogger.TOLERANCE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "core-app.in.topic",
                "core-app.out.topic",
                "core-app.downstream-ds1-override",
                "core-app.downstream-ds1-resend",
                "core-app.downstream-ds1-terminate",
                "core-app.downstream-ds1-forceack",
                "core-app.store-downstream-ds1-request_data_originals-inject",
                "core-app.store-downstream-ds1-request_data_overrides-inject",
                "core-app.store-downstream-ds1-requests-inject"
        },
        consumers = {
                "core-app.out.topic",
                "core-app.store-kv-changelog",
                "core-app.store-downstream-ds1-requests-changelog",
                "core-app.store-downstream-ds1-request_data_overrides-changelog",
                "core-app.store-downstream-ds1-request_data_originals-changelog"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSToleranceTest.ProcessorTopologyConfig.class
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class KSToleranceTest extends BaseKStreamApplicationTests {

    public static final Topic<Long, Long> SOURCE = new Topic<>("core-app.in.topic", Serdes.Long(), Serdes.Long());
    public static final Topic<Long, Long> SINK = new Topic<>("core-app.out.topic", Serdes.Long(), Serdes.Long());
    public static final Topic<Long, Long> CHANGELOG = new Topic<>("core-app.store-kv-changelog", Serdes.Long(), Serdes.Long());
    public static final Topic<Long, Long> OVERRIDE = new Topic<>("core-app.downstream-ds1-override", Serdes.Long(), Serdes.Long());

    @DynamicPropertySource
    public static void testConsumerTopics(DynamicPropertyRegistry registry) {
        registry.add(TOLERANCE_CONFIG, () -> 1);
    }

    @Configuration
    public static class ProcessorTopologyConfig {
        public static class ForwardProcessor implements ExtendedProcessor<Long, Long, Long, Long> {

            private KeyValueStore<Long, Long> store;
            private Downstream<Long, Long, Long> ds1;

            @Override
            public void init(ExtendedProcessorContext<Long, Long, Long, Long> context) {
                store = context.getStateStore("kv");
                ds1 = context.getDownstream("ds1");
            }

            @Override
            public void process(Record<Long, Long> record) {
                // Store the value
                store.put(record.key(), record.value());
                
                // Send to downstream
                ds1.send(Request.RequestType.NEW, record.key(), 1, record.value());
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                StoreBuilder<KeyValueStore<Long, Long>> store = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("kv"),
                        Serdes.Long(),
                        Serdes.Long()
                );

                DownstreamDefinition<Long, Long, Long> ds1 = new DownstreamDefinition<>(
                    "ds1",
                    Serdes.Long(),
                    new CorrelationIdGenerator<>(){},
                        new NewCancelConverter<>() {
                            @Override
                            public Record<Long, Long> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), rd, System.currentTimeMillis());
                            }

                            @Override
                            public Record<Long, Long> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), rd, System.currentTimeMillis());
                            }
                        },
                    new KSTopology.SinkDefinition<>(SINK, null, null),
                    null,
                    (toOverride, override) -> override
                );

                builder
                        .addProcessor(ForwardProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .withDownstream(ds1)
                        .done();
            };
        }
    }

    @BeforeEach
    void setup() {
        clearAllTopics();
    }

    @Test
    void testProcessing() {
        // Send record to source topic
        send(SOURCE, 1L, 100L);

        // Check store changelog
        ConsumerRecord<Long, Long> changed = waitForRecordFrom(CHANGELOG);
        assertEquals(1L, changed.key());
        assertEquals(100L, changed.value());

        // Check downstream
        ConsumerRecord<Long, Long> forwarded = waitForRecordFrom(SINK);
        assertEquals(100L, forwarded.value());

        // Send another record
        send(SOURCE, 2L, 200L);

        // Check store changelog again
        changed = waitForRecordFrom(CHANGELOG);
        assertEquals(200L, changed.value());

        // Check downstream again
        forwarded = waitForRecordFrom(SINK);
        assertEquals(200L, forwarded.value());
    }

    @Test
    void testCorruptedOverride() {
        // First send normal record
        send(SOURCE, 1L, 100L);

        // Check store changelog
        ConsumerRecord<Long, Long> changed = waitForRecordFrom(CHANGELOG);
        assertEquals(100L, changed.value());

        // Check downstream
        ConsumerRecord<Long, Long> forwarded = waitForRecordFrom(SINK);
        assertEquals(100L, forwarded.value());

        // Send corrupted override
        send(OVERRIDE.topic, OVERRIDE.keySerde.serializer().serialize(null,1L), "this isn't long for sure!!!111".getBytes(StandardCharsets.UTF_8));

        // System should continue working - send another normal record
        send(SOURCE, 2L, 200L);

        // Check store changelog still works
        changed = waitForRecordFrom(CHANGELOG);
        assertEquals(200L, changed.value());

        // Check downstream still works
        forwarded = waitForRecordFrom(SINK);
        assertEquals(200L, forwarded.value());
    }
} 