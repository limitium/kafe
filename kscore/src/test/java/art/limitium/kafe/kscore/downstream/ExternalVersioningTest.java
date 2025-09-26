package art.limitium.kafe.kscore.downstream;

import art.limitium.kafe.kscore.KStreamApplication;
import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.KSTopology;
import art.limitium.kafe.kscore.kstreamcore.KStreamInfraCustomizer;
import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.downstream.DownstreamDefinition;
import art.limitium.kafe.kscore.kstreamcore.downstream.DownstreamReplyProcessor;
import art.limitium.kafe.kscore.kstreamcore.downstream.converter.AmendConverter;
import art.limitium.kafe.kscore.kstreamcore.downstream.converter.CorrelationIdGenerator;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.test.BaseKStreamApplicationTests;
import art.limitium.kafe.kscore.test.KafkaTest;
import art.limitium.kafe.ksmodel.downstream.Request;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaTest(
        topics = {
                "core-app.in.topic",
                "core-app.out1.topic",
                "core-app.out2.topic",
                "core-app.r1.topic",
                "core-app.r2.topic",
                "core-app.downstream-ds1-override",
                "core-app.downstream-ds1-resend",
                "core-app.downstream-ds1-terminate",
                "core-app.downstream-ds1-forceack",
                "core-app.store-downstream-ds1-request_data_originals-inject",
                "core-app.store-downstream-ds1-request_data_overrides-inject",
                "core-app.store-downstream-ds1-requests-inject",
                "core-app.downstream-ds2-override",
                "core-app.downstream-ds2-resend",
                "core-app.downstream-ds2-terminate",
                "core-app.downstream-ds2-forceack",
                "core-app.store-downstream-ds2-request_data_originals-inject",
                "core-app.store-downstream-ds2-request_data_overrides-inject",
                "core-app.store-downstream-ds2-requests-inject"
        },
        consumers = {
                "core-app.out1.topic",
                "core-app.out2.topic",
                "core-app.store-downstream-ds1-requests-changelog",
                "core-app.store-downstream-ds1-request_data_overrides-changelog",
                "core-app.store-downstream-ds1-request_data_originals-changelog",
                "core-app.store-downstream-ds2-requests-changelog",
                "core-app.store-downstream-ds2-request_data_overrides-changelog",
                "core-app.store-downstream-ds2-request_data_originals-changelog"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                ExternalVersioningTest.ProcessorTopologyConfig.class
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class ExternalVersioningTest extends BaseKStreamApplicationTests {

    public static final Topic<Long, Long> SOURCE = new Topic<>("core-app.in.topic", Serdes.Long(), Serdes.Long());
    public static final Topic<Long, String> REPLY1 = new Topic<>("core-app.r1.topic", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> REPLY2 = new Topic<>("core-app.r2.topic", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> SINK1 = new Topic<>("core-app.out1.topic", Serdes.Long(), Serdes.String());
    public static final Topic<Long, String> SINK2 = new Topic<>("core-app.out2.topic", Serdes.Long(), Serdes.String());

    @Configuration
    public static class ProcessorTopologyConfig {
        public static class ForwardProcessor implements ExtendedProcessor<Long, Long, Long, String> {

            private KeyValueStore<Long, String> store;
            private Downstream<Long, Long, String> ds1;
            private Downstream<Long, Long, String> ds2;

            @Override
            public void init(ExtendedProcessorContext<Long, Long, Long, String> context) {
                store = context.getStateStore("kv");
                ds1 = context.getDownstream("ds1");
                ds2 = context.getDownstream("ds2");
            }

            @Override
            public void process(Record<Long, Long> record) {
                // Store the value
                store.put(record.key(), "");

                // Send to downstream
                int version = 1;
                Long id = record.key();

                ds1.send(Request.RequestType.NEW, id, version, record.value());
                ds2.send(Request.RequestType.NEW, id, version, record.value());
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> {
                StoreBuilder<KeyValueStore<Long, String>> store = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("kv"),
                        Serdes.Long(),
                        Serdes.String()
                );

                DownstreamDefinition<Long, Long, String> ds1 = new DownstreamDefinition<>(
                        "ds1",
                        Serdes.Long(),
                        new CorrelationIdGenerator<>() {
                        },
                        new AmendConverter<>() {
                            @Override
                            public Record<Long, String> amendRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }

                            @Override
                            public Record<Long, String> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }

                            @Override
                            public Record<Long, String> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }
                        },
                        new KSTopology.SinkDefinition<>(SINK1, null, null),
                        new DownstreamDefinition.ReplyDefinition<>(new KSTopology.SourceDefinition<>(REPLY1, false), new DownstreamReplyProcessor.ReplyConsumer<Long, String>() {
                            @Override
                            public void onReply(Record<Long, String> record, DownstreamReplyProcessor.RequestsAware requestsAware) {
                                String[] split = record.value().split("-");
                                String correlationId = split[0];
                                String extId = split[1];
                                String extVersion = split[2];
                                requestsAware.replied(correlationId, true, null, null, extId, Integer.parseInt(extVersion));
                            }
                        }),
                        (toOverride, override) -> override
                );

                DownstreamDefinition<Long, Long, String> ds2 = new DownstreamDefinition<>(
                        "ds2",
                        Serdes.Long(),
                        new CorrelationIdGenerator<>() {
                        },
                        new AmendConverter<>() {
                            @Override
                            public Record<Long, String> amendRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }

                            @Override
                            public Record<Long, String> newRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }

                            @Override
                            public Record<Long, String> cancelRequest(String correlationId, long effectiveReferenceId, int effectiveReferenceVersion, Long rd, String eId, int eV) {
                                return new Record<>(Long.parseLong(correlationId), String.join("-",String.valueOf(rd), correlationId, eId, String.valueOf(eV)), System.currentTimeMillis());
                            }
                        },
                        new KSTopology.SinkDefinition<>(SINK2, null, null),
                        new DownstreamDefinition.ReplyDefinition<>(new KSTopology.SourceDefinition<>(REPLY2, false), new DownstreamReplyProcessor.ReplyConsumer<Long, String>() {
                            @Override
                            public void onReply(Record<Long, String> record, DownstreamReplyProcessor.RequestsAware requestsAware) {
                                String[] split = record.value().split("-");
                                String correlationId = split[0];
                                String extId = split[1];
                                String extVersion = split[2];
                                requestsAware.replied(correlationId, true, null, null, extId, Integer.parseInt(extVersion));
                            }
                        }),
                        (toOverride, override) -> override
                );

                builder
                        .addProcessor(ForwardProcessor::new)
                        .withSource(SOURCE)
                        .withStores(store)
                        .withDownstream(ds1)
                        .withDownstream(ds2)
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
        // This triggers the ForwardProcessor which will:
        // 1. Store the value in the key-value store
        // 2. Send NEW requests to both downstream services (ds1 and ds2)
        // 3. Each downstream will generate a correlation ID and send to their respective sink topics
        send(SOURCE, 1L, 100L);

        // Check downstream 1
        // Expected: A record should appear in SINK1 with format "value-correlationId-extId-extVersion"
        // The ForwardProcessor sends the original value (100L) to ds1, which gets formatted by AmendConverter
        String[] split1 = waitForRecordFrom(SINK1).value().split("-");
        long val1 = Long.parseLong(split1[0]);
        String correlationId1 = split1[1];
        String extId1 = split1[2];
        int extVersion1 = Integer.parseInt(split1[3]);
        assertEquals(100L, val1);

        // Check downstream 2
        // Expected: A record should appear in SINK2 with format "value-correlationId-extId-extVersion"
        // The ForwardProcessor sends the original value (100L) to ds2, which gets formatted by AmendConverter
        String[] split2 = waitForRecordFrom(SINK2).value().split("-");
        long val2 = Long.parseLong(split2[0]);
        String correlationId2 = split2[1];
        String extId2 = split2[2];
        int extVersion2 = Integer.parseInt(split2[3]);
        assertEquals(100L, val2);

        // Send another record
        // This will trigger the same flow again with a new value (200L)
        // Both downstream services should receive this new request
        send(SOURCE, 1L, 200L);

    }


} 