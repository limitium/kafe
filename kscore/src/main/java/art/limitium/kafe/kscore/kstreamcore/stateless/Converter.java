package art.limitium.kafe.kscore.kstreamcore.stateless;

import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Defines the most basic task with convertation incoming Record<KIn, VIn> into outgoing Record<KOut, VOut>
 *
 * @param <KIn>
 * @param <VIn>
 * @param <KOut>
 * @param <VOut>
 * @param <DLQm>
 */
public interface Converter<KIn, VIn, KOut, VOut, DLQm> extends Base<KIn, VIn, KOut, VOut, DLQm> {
    /**
     * Business related convertation problem.
     */
    class ConvertException extends RuntimeException {

        /**
         *
         * @param msg passed to DLQ
         * @param cause
         */
        public ConvertException(String msg, Exception cause) {
            super(msg, cause);
        }

        public ConvertException(String msg) {
            super(msg);
        }
    }

    /**
     * Converts incoming record from {@link Base#inputTopic()} into outgoing record to {@link Base#outputTopic()}
     *
     * @param toConvert incoming record for convertation
     * @param context to work with metadata
     * @return new record to be sent or null to skip sending
     * @throws ConvertException real business exceptions which must be settled into {@link Base#dlq()}
     */
    Record convert(Record<KIn, VIn> toConvert, ExtendedProcessorContext<KIn, VIn, KOut, VOut> context) throws ConvertException;
}
