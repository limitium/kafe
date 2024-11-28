package art.limitium.kafe.kscore.kstreamcore.downstream;

public interface RequestDataOverrider<RequestData> {
    RequestData override(RequestData original, RequestData override);
}
