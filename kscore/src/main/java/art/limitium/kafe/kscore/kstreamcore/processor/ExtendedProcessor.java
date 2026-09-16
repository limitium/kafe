package art.limitium.kafe.kscore.kstreamcore.processor;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.time.LocalDate;

public interface ExtendedProcessor<KIn, VIn, KOut, VOut> {

    /**
     * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
     * that contains it is initialized. When the framework is done with the processor, {@link #close()} will be called on it; the
     * framework may later re-use the processor by calling {@code #init()} again.
     * <p>
     * The provided {@link ExtendedProcessorContext context} can be used to access topology and record meta data, to
     * {@link ExtendedProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link StateStore}s.
     *
     * @param context the context; may not be null
     */
    default void init(final ExtendedProcessorContext<KIn, VIn, KOut, VOut> context) {
    }

    /**
     * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
     *
     * @param record the record to process
     */
    void process(Record<KIn, VIn> record);

    /**
     * Evaluate the trade date bound to the incoming message. Called once per record, before
     * {@link #process(Record)} is entered, and exposed for the whole duration of the processing through
     * {@link ExtendedProcessorContext#getTradeDate()}.
     * <p>
     * Default evaluation derives the date from the incoming record timestamp, see
     * {@link TradeDateEvaluator#fromRecordTimestamp(Record)}. Override to supply a business rule.
     * <p>
     * Returning null, or throwing, routes the record to the DLQ. Throwing a
     * {@link art.limitium.kafe.kscore.kstreamcore.dlq.DLQException} keeps its own message and key/subKey in the DLQ
     * envelope, anything else is wrapped into a {@link TradeDateEvaluationException}.
     *
     * @param incoming incoming message
     * @param context  context bound to the incoming message
     * @return trade date, never null
     */
    default LocalDate evaluateTradeDate(Record<KIn, VIn> incoming, ExtendedProcessorContext<KIn, VIn, KOut, VOut> context) {
        return TradeDateEvaluator.fromRecordTimestamp(incoming);
    }

    /**
     * Close this processor and clean up any resources. Be aware that {@code #close()} is called after an internal cleanup.
     * Thus, it is not possible to write anything to Kafka as underlying clients are already closed. The framework may
     * later re-use this processor by calling {@code #init()} on it again.
     * <p>
     * Note: Do not close any streams managed resources, like {@link StateStore}s here, as they are managed by the library.
     */
    default void close() {
    }
}
