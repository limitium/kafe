package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamForceAckProcessor implements ExtendedProcessor<String, String, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamForceAckProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<String, String, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<String, String> record) {
        downstream.forceAckRequest(record.key());
    }
}
