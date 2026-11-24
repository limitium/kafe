package art.limitium.kafe.kscore.kstreamcore.stateless;

/**
 * Defines custom partitioner for outgoing Record<KOut, VOut> to {@link Base#outputTopic()}
 *
 * @param <KIn>
 * @param <VIn>
 * @param <KOut>
 * @param <VOut>
 * @param <DLQm>
 */
public interface Partitioner<KIn, VIn, KOut, VOut, DLQm> extends Base<KIn, VIn, KOut, VOut, DLQm> {
    /**
     * Repartionate {@link Base#outputTopic()} with the same signature as {@link art.limitium.kafe.kscore.kstreamcore.SinglePartitioner#partition(String, Object, Object, int)},
     * based on outgoing Record<KOut, VOut> key and value
     *
     * @param topic
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    int partition(String topic, KOut key, VOut value, int numPartitions);
}
