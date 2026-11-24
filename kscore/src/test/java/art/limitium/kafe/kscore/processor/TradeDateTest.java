package art.limitium.kafe.kscore.processor;

import art.limitium.kafe.kscore.kstreamcore.dlq.DLQException;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ProcessorMeta;
import art.limitium.kafe.kscore.kstreamcore.processor.TradeDateEvaluationException;
import art.limitium.kafe.kscore.kstreamcore.processor.TradeDateEvaluator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TradeDateTest {

    public static final long MILLIS_2026_09_16 = 1789560000000L; //2026-09-16T12:00:00Z

    /**
     * Widens {@link ExtendedProcessorContext#beginProcessing(Record)} for testing, the way
     * {@link art.limitium.kafe.kscore.audit.AuditWrapperSupplierTest.TestableExtendedProcessorContext} does.
     */
    public static class TestableExtendedProcessorContext extends ExtendedProcessorContext<Integer, String, Integer, String> {

        public TestableExtendedProcessorContext(TradeDateEvaluator<Integer, String, Integer, String> tradeDateEvaluator) {
            super(new StubProcessorContext(), null, tradeDateEvaluator);
        }

        public TestableExtendedProcessorContext() {
            super(new StubProcessorContext(), null);
        }

        @Override
        public void beginProcessing(Record<Integer, String> incomingRecord) {
            super.beginProcessing(incomingRecord);
        }
    }

    @Test
    void defaultEvaluationDerivesTradeDateFromRecordTimestamp() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext();

        context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16));

        assertEquals(LocalDate.of(2026, 9, 16), context.getTradeDate());
    }

    @Test
    void defaultEvaluationIsReplaySafe() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext();

        context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16));
        LocalDate first = context.getTradeDate();
        context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16));

        assertEquals(first, context.getTradeDate());
    }

    @Test
    void overriddenEvaluationIsHonoured() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext((incoming, ctx) -> LocalDate.parse(incoming.value()));

        context.beginProcessing(new Record<>(1, "2020-01-31", MILLIS_2026_09_16));

        assertEquals(LocalDate.of(2020, 1, 31), context.getTradeDate());
    }

    @Test
    void failedEvaluationDoesNotLeakPreviousTradeDate() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext((incoming, ctx) -> LocalDate.parse(incoming.value()));

        context.beginProcessing(new Record<>(1, "2020-01-31", MILLIS_2026_09_16));
        assertEquals(LocalDate.of(2020, 1, 31), context.getTradeDate());

        assertThrows(TradeDateEvaluationException.class, () -> context.beginProcessing(new Record<>(2, "not-a-date", MILLIS_2026_09_16)));

        assertNull(context.getTradeDate(), "previous record trade date must not leak into the DLQ envelope");
    }

    @Test
    void throwingEvaluationIsWrappedWithAReason() {
        RuntimeException cause = new IllegalStateException("no calendar");
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext((incoming, ctx) -> {
            throw cause;
        });

        TradeDateEvaluationException thrown = assertThrows(TradeDateEvaluationException.class,
                () -> context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16)));

        assertSame(cause, thrown.getCause());
        assertTrue(thrown.getMessage().contains("no calendar"), thrown.getMessage());
        assertNull(thrown.getKey());
        assertNull(thrown.getSubKey());
    }

    @Test
    void nullEvaluationIsRejected() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext((incoming, ctx) -> null);

        TradeDateEvaluationException thrown = assertThrows(TradeDateEvaluationException.class,
                () -> context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16)));

        assertEquals("Trade date evaluator returned null", thrown.getMessage());
    }

    @Test
    void evaluatorOwnDLQExceptionPropagatesUnwrapped() {
        DLQException own = new DLQException("no book", "TRADE_DATE", "MISSING_BOOK");
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext((incoming, ctx) -> {
            throw own;
        });

        DLQException thrown = assertThrows(DLQException.class,
                () -> context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16)));

        assertSame(own, thrown);
        assertEquals("TRADE_DATE", thrown.getKey());
        assertEquals("MISSING_BOOK", thrown.getSubKey());
    }

    @Test
    void evaluationCanReadTheContext() {
        TestableExtendedProcessorContext context = new TestableExtendedProcessorContext(
                (incoming, ctx) -> LocalDate.ofEpochDay(ctx.taskId().partition()));

        context.beginProcessing(new Record<>(1, "v", MILLIS_2026_09_16));

        assertEquals(LocalDate.ofEpochDay(1), context.getTradeDate());
    }

    /**
     * Minimal {@link ProcessorContext}, only taskId() and appConfigs() are read while constructing the context.
     */
    static class StubProcessorContext implements ProcessorContext<Integer, String> {
        @Override
        public <K extends Integer, V extends String> void forward(Record<K, V> record) {
        }

        @Override
        public <K extends Integer, V extends String> void forward(Record<K, V> record, String childName) {
        }

        @Override
        public String applicationId() {
            return null;
        }

        @Override
        public TaskId taskId() {
            return new TaskId(1, 1);
        }

        @Override
        public Optional<RecordMetadata> recordMetadata() {
            return Optional.empty();
        }

        @Override
        public Serde<?> keySerde() {
            return null;
        }

        @Override
        public Serde<?> valueSerde() {
            return null;
        }

        @Override
        public File stateDir() {
            return null;
        }

        @Override
        public StreamsMetrics metrics() {
            return null;
        }

        @Override
        public <S extends StateStore> S getStateStore(String name) {
            return null;
        }

        @Override
        public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
            return null;
        }

        @Override
        public Cancellable schedule(Instant startTime, Duration interval, PunctuationType type, Punctuator callback) {
            return null;
        }

        @Override
        public void commit() {
        }

        @Override
        public Map<String, Object> appConfigs() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Object> appConfigsWithPrefix(String prefix) {
            return null;
        }

        @Override
        public long currentSystemTimeMs() {
            return 0;
        }

        @Override
        public long currentStreamTimeMs() {
            return 0;
        }
    }
}
