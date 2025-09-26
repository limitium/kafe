package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Downstream;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessor;
import art.limitium.kafe.kscore.kstreamcore.processor.ExtendedProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownstreamResendProcessor implements ExtendedProcessor<Long, String, Object, Object> {
    private static final Logger logger = LoggerFactory.getLogger(DownstreamResendProcessor.class);
    
    public static String CORRECT = "CORRECT";

    final String downstreamName;
    private Downstream<Object, Object, Object> downstream;

    public DownstreamResendProcessor(String downstreamName) {
        this.downstreamName = downstreamName;
    }

    @Override
    public void init(ExtendedProcessorContext<Long, String, Object, Object> context) {
        downstream = context.getDownstream(downstreamName);
    }

    @Override
    public void process(Record<Long, String> record) {
        try {
            if (CORRECT.equals(record.value())) {
                downstream.correctRequest(record.key());
            } else {
                downstream.resendRequest(record.key());
            }
        } catch (Exception e) {
            logger.error("Failed to process resend/correct request for key: {}, value: {}, downstream: {}", 
                        record.key(), record.value(), downstreamName, e);
            // Log error but continue processing - don't fail the entire stream
        }
    }
}
