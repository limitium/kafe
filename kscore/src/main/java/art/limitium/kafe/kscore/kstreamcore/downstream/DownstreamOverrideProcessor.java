package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamOverrideProcessor<RequestData> implements ExtendedProcessor<Long, RequestData, Object, Object> {
    private static final Logger logger = LoggerFactory.getLogger(DownstreamOverrideProcessor.class);
    
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
        try {
            downstream.updateOverride(record.key(), record.value());
        } catch (Exception e) {
            logger.error("Failed to update override for key: {}, downstream: {}", record.key(), downstreamName, e);
            // Log error but continue processing - don't fail the entire stream
        }
    }
}
