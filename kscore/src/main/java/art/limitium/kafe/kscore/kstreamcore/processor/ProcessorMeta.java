package art.limitium.kafe.kscore.kstreamcore.processor;

import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.kscore.kstreamcore.dlq.DLQTransformer;
import art.limitium.kafe.kscore.kstreamcore.downstream.DownstreamDefinition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProcessorMeta<KIn, VIn, KOut, VOut> {
    public String name;
    public Topic<KIn, ?> dlqTopic;
    public DLQTransformer<KIn, VIn, ?> dlqTransformer;

    public Map<String, DownstreamDefinition<?, ? extends KOut, ? extends VOut>> downstreamDefinitions = new HashMap<>();

    public Set<String> storeNames = new HashSet<>();
}
