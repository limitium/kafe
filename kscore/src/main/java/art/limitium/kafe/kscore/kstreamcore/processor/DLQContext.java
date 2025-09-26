package art.limitium.kafe.kscore.kstreamcore.processor;

/**
 * Context interface for Dead Letter Queue (DLQ) operations in Kafka Streams.
 * 
 * This interface provides access to contextual information needed for DLQ processing,
 * including sequence numbers, topic metadata, and timing information. It is used
 * by DLQ transformers and processors to maintain proper ordering and tracking
 * of messages that need to be sent to a dead letter queue.
 */
public interface DLQContext {
    
    /**
     * Gets the next sequence number for DLQ message ordering.
     * 
     * @return the next sequence number in the sequence
     */
    long getNextSequence();

    /**
     * Gets the name of the Kafka topic being processed.
     * 
     * @return the topic name
     */
    String getTopic();

    /**
     * Gets the offset of the current message in the Kafka partition.
     * 
     * @return the message offset
     */
    long getOffset();

    /**
     * Gets the partition number of the current message.
     * 
     * @return the partition number
     */
    int getPartition();

    /**
     * Gets the current local time in milliseconds.
     * 
     * @return current time in milliseconds since epoch
     */
    long currentLocalTimeMs();
}
