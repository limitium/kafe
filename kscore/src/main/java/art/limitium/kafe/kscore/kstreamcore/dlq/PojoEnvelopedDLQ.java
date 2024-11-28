package art.limitium.kafe.kscore.kstreamcore.dlq;

import art.limitium.kafe.kscore.kstreamcore.Topic;

public class PojoEnvelopedDLQ<KIn, VIn> extends EnvelopedDLQ<KIn, VIn> {
    public PojoEnvelopedDLQ(DLQTopic<KIn, VIn> dlqTopic) {
        super(dlqTopic, new PojoDLQTransformer<>());
    }

    public PojoEnvelopedDLQ(Topic<KIn, VIn> sourceTopic, String dlqTopic) {
        this(DLQTopic.createFor(sourceTopic, dlqTopic));
    }
}
