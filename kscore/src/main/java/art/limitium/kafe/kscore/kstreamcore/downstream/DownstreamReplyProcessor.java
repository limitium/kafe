package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;

import javax.annotation.Nullable;

public class DownstreamReplyProcessor<KeyType, ReplyType> implements ExtendedProcessor<KeyType, ReplyType, Object, Object> {
    final String downstreamName;
    final ReplyConsumer<KeyType, ReplyType> replyConsumer;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamReplyProcessor(String downstreamName, ReplyConsumer<KeyType, ReplyType> replyConsumer) {
        this.downstreamName = downstreamName;
        this.replyConsumer = replyConsumer;
    }

    @Override
    public void init(ExtendedProcessorContext<KeyType, ReplyType, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<KeyType, ReplyType> record) {
        replyConsumer.onReply(record, downstream::requestReplied);
    }

    public interface ReplyConsumer<KeyType, ReplyType> {
        void onReply(Record<KeyType, ReplyType> record, RequestsAware requestsAware);
    }

    public interface RequestsAware {
        void replied(String correlationId, boolean isAck, @Nullable String code, @Nullable String answer, @Nullable String externalId, int externalVersion);

        default void replied(String correlationId, boolean isAck, @Nullable String code, @Nullable String answer) {
            this.replied(correlationId, isAck, code, answer, null, 0);
        }

        default void replied(String correlationId, boolean isAck) {
            this.replied(correlationId, isAck, null, null);
        }
    }
}
