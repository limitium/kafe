package art.limitium.kafe.kscore.kstreamcore.dlq;

import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.ksmodel.store.WrappedValue;
import art.limitium.kafe.ksmodel.store.WrapperValueSerde;
import org.apache.kafka.common.serialization.Serde;

public class DLQTopic<K, V> extends Topic<K, WrappedValue<DLQEnvelope, V>> {
    public DLQTopic(String dlqTopic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(dlqTopic, keySerde, new WrapperValueSerde<>(new DLQEnvelope.DLQEnvelopeSerde(), valueSerde));
    }

    public static <K, V> DLQTopic<K, V> createFor(Topic<K, V> topic, String dlqTopic) {
        return new DLQTopic<>(dlqTopic, topic.keySerde, topic.valueSerde);
    }
}
