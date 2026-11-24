package art.limitium.kafe.kscore.kstreamcore;

import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Optional;
import java.util.Set;

/**
 * {@link StreamPartitioner} that sends every record to one partition, the shape {@code StreamPartitioner} had before Kafka 4.
 * <p>
 * Kafka 4 kept only {@link StreamPartitioner#partitions(String, Object, Object, int)}, which returns a set of partitions so a record
 * can be multicast. This interface turns a single partition back into that set, so a sink partitioner stays a one-liner:
 * <pre>{@code
 * new SinkDefinition<>(topic, null, SinglePartitioner.of((topic, key, value, numPartitions) -> Sequencer.getPartition(key)))
 * }</pre>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface SinglePartitioner<K, V> extends StreamPartitioner<K, V> {

    /**
     * @return partition for the record, or {@code null} to let Kafka's default partitioner choose
     */
    Integer partition(String topic, K key, V value, int numPartitions);

    @Override
    default Optional<Set<Integer>> partitions(String topic, K key, V value, int numPartitions) {
        return Optional.ofNullable(partition(topic, key, value, numPartitions)).map(Set::of);
    }

    /**
     * Lets a lambda or method reference be used where a {@link StreamPartitioner} is expected, without a cast.
     */
    static <K, V> SinglePartitioner<K, V> of(SinglePartitioner<K, V> partitioner) {
        return partitioner;
    }
}
