package art.limitium.kafe.kscore.kstreamcore.dlq;

import art.limitium.kafe.kscore.kstreamcore.Topic;

public abstract class DLQ<KIn, VIn, DLQm> {
    public DLQ(Topic<KIn, DLQm> topic, DLQTransformer<KIn, VIn, DLQm> transformer) {
        this.topic = topic;
        this.transformer = transformer;
    }

    public Topic<KIn, DLQm> topic;
    public DLQTransformer<KIn, VIn, DLQm> transformer;
}
