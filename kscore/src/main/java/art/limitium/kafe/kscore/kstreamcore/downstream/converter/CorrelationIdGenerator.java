package art.limitium.kafe.kscore.kstreamcore.downstream.converter;


import art.limitium.kafe.ksmodel.downstream.Request;

public interface CorrelationIdGenerator<RequestData> {
    default String generate(long requestId, Request.RequestType requestType, RequestData requestData, Long traceId, int partition) {
        return String.valueOf(requestId);
    }
}
