package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamTerminateProcessor implements ExtendedProcessor<Long, Long, Object, Object> {
    private static final Logger logger = LoggerFactory.getLogger(DownstreamTerminateProcessor.class);
    
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
        try {
            downstream.terminateRequest(record.key(), record.value());
        } catch (Exception e) {
            logger.error("Failed to terminate request for key: {}, value: {}, downstream: {}", 
                        record.key(), record.value(), downstreamName, e);
            // Log error but continue processing - don't fail the entire stream
        }
    }
}
