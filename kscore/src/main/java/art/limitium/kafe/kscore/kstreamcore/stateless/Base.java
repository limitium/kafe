package art.limitium.kafe.kscore.kstreamcore.stateless;

import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQ;

/**
 * Common definition for stateless process, must be used at least with {@link Converter} or {@link Partitioner}
 *
 * @param <KIn>
 * @param <VIn>
 * @param <KOut>
 * @param <VOut>
 * @param <DLQm>
 */
public interface Base<KIn, VIn, KOut, VOut, DLQm> {
    /**
     * Defines source topic for incoming messages
     *
     * @return
     */
    Topic<KIn, VIn> inputTopic();

    /**
     * Defines sink topic for outgoing messages
     * @return
     */
    Topic<KOut, VOut> outputTopic();

    /**
     * Defines dlq topic and transformer for failed convertations
     * @return
     */
    default DLQ<KIn, VIn, DLQm> dlq(){
        return null;
    }
}
