package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class DownstreamTerminateProcessor implements ExtendedProcessor<Long, Long, Object, Object> {
    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamTerminateProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, Long, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, Long> record) {
        downstream.terminateRequest(record.key(), record.value());
    }
}
