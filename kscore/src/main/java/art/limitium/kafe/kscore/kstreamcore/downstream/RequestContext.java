package art.limitium.kafe.kscore.kstreamcore.downstream;

import art.limitium.kafe.ksmodel.downstream.Request.RequestType;

public class RequestContext<RequestData> {
    public RequestType requestType;
    public long referenceId;
    public int referenceVersion;
    public int overrideVersion;

    public RequestData requestData;

    public RequestContext(RequestType requestType, long referenceId, int referenceVersion, int overrideVersion, RequestData requestData) {
        this.requestType = requestType;
        this.referenceId = referenceId;
        this.referenceVersion = referenceVersion;
        this.overrideVersion = overrideVersion;
        this.requestData = requestData;
    }

}
