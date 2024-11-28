package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamOverrideProcessor<RequestData> implements ExtendedProcessor<Long, RequestData, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamOverrideProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, RequestData, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, RequestData> record) {
        downstream.updateOverride(record.key(), record.value());
    }
}
