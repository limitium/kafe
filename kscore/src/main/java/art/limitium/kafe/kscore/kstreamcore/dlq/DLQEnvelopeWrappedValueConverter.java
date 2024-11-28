package art.limitium.kafe.kscore.kstreamcore.dlq;


import art.limitium.kafe.ksmodel.store.WrappedConverter;
import art.limitium.kafe.ksmodel.store.WrappedValueConverter;

public abstract class DLQEnvelopeWrappedValueConverter
    <M> extends WrappedValueConverter<DLQEnvelope, M> {
    @Override
    protected WrappedConverter<DLQEnvelope> getWrappedConverter() {
        return new DLQEnvelopeWrappedConverter();
    }
}
