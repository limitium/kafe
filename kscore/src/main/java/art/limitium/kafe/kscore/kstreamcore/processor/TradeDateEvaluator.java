package art.limitium.kafe.kscore.kstreamcore.processor;

import org.apache.kafka.streams.processor.api.Record;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Evaluates the trade date bound to an incoming message.
 * <p>
 * Evaluated once per incoming record, before {@link ExtendedProcessor#process(Record)} is entered,
 * and exposed for the whole duration of the processing through {@link ExtendedProcessorContext#getTradeDate()}.
 *
 * @param <KIn>  type of incoming record key
 * @param <VIn>  type of incoming record value
 * @param <KOut> type of outgoing record key
 * @param <VOut> type of outgoing record value
 * @see ExtendedProcessor#evaluateTradeDate(Record, ExtendedProcessorContext)
 */
public interface TradeDateEvaluator<KIn, VIn, KOut, VOut> {
    /**
     * Zone the default evaluation collapses a timestamp into a date at. Kept in sync with {@link art.limitium.kafe.sequencer.Sequencer#ZONE_OFFSET}.
     */
    ZoneOffset TRADE_DATE_ZONE = ZoneOffset.UTC;

    /**
     * Evaluate the trade date for the incoming message.
     *
     * @param incoming incoming message
     * @param context  context bound to the incoming message
     * @return trade date, never null
     */
    LocalDate evaluate(Record<KIn, VIn> incoming, ExtendedProcessorContext<KIn, VIn, KOut, VOut> context);

    /**
     * Default evaluation - the incoming record timestamp collapsed to a date at {@link #TRADE_DATE_ZONE}.
     * Replay safe, reprocessing the same record yields the same trade date.
     *
     * @param incoming incoming message
     * @return trade date of the incoming message
     */
    static LocalDate fromRecordTimestamp(Record<?, ?> incoming) {
        return Instant.ofEpochMilli(incoming.timestamp()).atZone(TRADE_DATE_ZONE).toLocalDate();
    }
}
