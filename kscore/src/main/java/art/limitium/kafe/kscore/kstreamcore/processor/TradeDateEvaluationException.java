package art.limitium.kafe.kscore.kstreamcore.processor;

import art.limitium.kafe.kscore.kstreamcore.dlq.DLQException;

/**
 * Raised when the trade date can't be evaluated for an incoming message.
 * <p>
 * Carries no {@link DLQException#getKey() key} by default - the failure is identified downstream by
 * {@link art.limitium.kafe.kscore.kstreamcore.dlq.DLQEnvelope#exceptionClass()}, while the explanation lands in
 * {@link art.limitium.kafe.kscore.kstreamcore.dlq.DLQEnvelope#message()} and the cause in
 * {@link art.limitium.kafe.kscore.kstreamcore.dlq.DLQEnvelope#stacktrace()}.
 *
 * @see TradeDateEvaluator
 */
public class TradeDateEvaluationException extends DLQException {

    public TradeDateEvaluationException(String message) {
        super(message, null, null);
    }

    public TradeDateEvaluationException(String message, Throwable cause) {
        super(message, cause, null, null);
    }

    /**
     * For processors which already have an error code taxonomy.
     *
     * @param message human-readable explanation
     * @param cause   exception if occurred
     * @param key     error code
     * @param subKey  error code qualifier, optional
     */
    public TradeDateEvaluationException(String message, Throwable cause, String key, String subKey) {
        super(message, cause, key, subKey);
    }
}
