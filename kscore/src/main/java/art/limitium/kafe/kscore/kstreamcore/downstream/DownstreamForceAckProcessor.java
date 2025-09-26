package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamForceAckProcessor implements ExtendedProcessor<String, String, Object, Object> {
    private static final Logger logger = LoggerFactory.getLogger(DownstreamForceAckProcessor.class);
    
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
        try {
            downstream.forceAckRequest(record.key());
        } catch (Exception e) {
            logger.error("Failed to force ack request for key: {}, downstream: {}", record.key(), downstreamName, e);
            // Log error but continue processing - don't fail the entire stream
        }
    }
}
