package art.limitium.kafe.kscore;

import art.limitium.kafe.kscore.kstreamcore.KStreamInfraCustomizer;
import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQEnvelope;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQException;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQTopic;
import art.limitium.kafe.kscore.kstreamcore.dlq.PojoEnvelopedDLQ;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.TradeDateEvaluationException;
import art.limitium.kafe.kscore.test.BaseKStreamApplicationTests;
import art.limitium.kafe.kscore.test.KafkaTest;
import art.limitium.kafe.ksmodel.store.WrappedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KafkaTest(
        topics = {"test.in.td.1"},
        consumers = {
                "test.out.td.1",
                "test.out.td.dlq.1"
        },
        configs = {
                KStreamApplication.class,
                BaseKStreamApplicationTests.BaseKafkaTestConfig.class,
                KSTradeDateTest.TopologyConfig.class
        })
class KSTradeDateTest extends BaseKStreamApplicationTests {

    public static final Topic<Integer, String> SOURCE = new Topic<>("test.in.td.1", Serdes.Integer(), Serdes.String());
    public static final Topic<Integer, String> SINK = new Topic<>("test.out.td.1", Serdes.Integer(), Serdes.String());
    public static final DLQTopic<Integer, String> DLQ_TOPIC = DLQTopic.createFor(SOURCE, "test.out.td.dlq.1");
    public static final PojoEnvelopedDLQ<Integer, String> DLQ = new PojoEnvelopedDLQ<>(DLQ_TOPIC);

    public static final int KEY_OWN_DLQ_EXCEPTION = -7;

    @Configuration
    public static class TopologyConfig {

        /**
         * Evaluates the trade date out of the incoming payload, then forwards whatever the context resolved.
         */
        public static class TradeDateProcessor implements ExtendedProcessor<Integer, String, Integer, String> {
            private ExtendedProcessorContext<Integer, String, Integer, String> context;

            @Override
            public void init(ExtendedProcessorContext<Integer, String, Integer, String> context) {
                this.context = context;
            }

            @Override
            public LocalDate evaluateTradeDate(Record<Integer, String> incoming, ExtendedProcessorContext<Integer, String, Integer, String> context) {
                if (incoming.key() == KEY_OWN_DLQ_EXCEPTION) {
                    throw new DLQException("no book for " + incoming.value(), "TRADE_DATE", "MISSING_BOOK");
                }
                return LocalDate.parse(incoming.value());
            }

            @Override
            public void process(Record<Integer, String> record) {
                context.send(SINK, record.withValue(context.getTradeDate().toString()));
            }
        }

        @Bean
        public static KStreamInfraCustomizer.KStreamKSTopologyBuilder provideTopology() {
            return builder -> builder
                    .addProcessor(TradeDateProcessor::new)
                    .withSource(SOURCE)
                    .withSink(SINK)
                    .withDLQ(DLQ)
                    .done();
        }
    }

    @Test
    void overriddenEvaluatorIsResolvedBeforeProcess() {
        send(SOURCE, 1, "2020-01-31");

        ConsumerRecord<Integer, String> out = waitForRecordFrom(SINK);

        assertEquals(1, out.key());
        assertEquals("2020-01-31", out.value());
    }

    @Test
    void failedEvaluationLandsInDLQWithAReason() {
        send(SOURCE, 2, "not-a-date");

        ConsumerRecord<Integer, WrappedValue<DLQEnvelope, String>> out = waitForRecordFrom(DLQ_TOPIC);
        DLQEnvelope envelope = out.value().wrapper();

        assertEquals(2, out.key());
        assertEquals("not-a-date", out.value().value(), "original payload must survive into the DLQ");
        assertEquals(TradeDateEvaluationException.class.getCanonicalName(), envelope.exceptionClass());
        assertNotNull(envelope.message());
        assertTrue(envelope.message().startsWith("Unable to evaluate trade date:"), envelope.message());
        assertTrue(envelope.stacktrace().contains("DateTimeParseException"), envelope.stacktrace());
        assertEquals(SOURCE.topic, envelope.sourceTopic());
    }

    @Test
    void evaluatorOwnDLQExceptionKeepsItsCodes() {
        send(SOURCE, KEY_OWN_DLQ_EXCEPTION, "ignored");

        ConsumerRecord<Integer, WrappedValue<DLQEnvelope, String>> out = waitForRecordFrom(DLQ_TOPIC);
        DLQEnvelope envelope = out.value().wrapper();

        assertEquals(KEY_OWN_DLQ_EXCEPTION, out.key());
        assertEquals(DLQException.class.getCanonicalName(), envelope.exceptionClass());
        assertEquals("no book for ignored", envelope.message());
        assertEquals("TRADE_DATE", envelope.exceptionKey());
        assertEquals("MISSING_BOOK", envelope.exceptionSubKey());
    }
}
