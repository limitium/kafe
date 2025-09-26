package art.limitium.kafe.kscore.kstreamcore.downstream.state;

public class DownstreamReferenceState {
    public ReferenceState state;
    public long effectiveReferenceId;
    public int effectiveReferenceVersion;
    public String id;
    public int version;

    public DownstreamReferenceState(ReferenceState state, long effectiveReferenceId, int effectiveReferenceVersion, String id, int version) {
        this.state = state;
        this.effectiveReferenceId = effectiveReferenceId;
        this.effectiveReferenceVersion = effectiveReferenceVersion;
        this.id = id;
        this.version = version;
    }

    @Override
    public String toString() {
        return "DownstreamReferenceState{" +
                "state=" + state +
                ", effectiveReferenceId=" + effectiveReferenceId +
                ", effectiveReferenceVersion=" + effectiveReferenceVersion +
                '}';
    }

    public enum ReferenceState {
        UNAWARE, EXISTS, CANCELED
    }
}
