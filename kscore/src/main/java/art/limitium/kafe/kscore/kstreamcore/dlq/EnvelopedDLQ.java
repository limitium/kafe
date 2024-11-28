package art.limitium.kafe.kscore.kstreamcore.dlq;


import art.limitium.kafe.ksmodel.store.WrappedValue;

public class EnvelopedDLQ<KIn, VIn> extends DLQ<KIn, VIn, WrappedValue<DLQEnvelope, VIn>> {
    public EnvelopedDLQ(DLQTopic<KIn, VIn> dlqTopic, DLQTransformer<KIn, VIn, WrappedValue<DLQEnvelope, VIn>> transformer) {
        super(dlqTopic, transformer);
    }
}
