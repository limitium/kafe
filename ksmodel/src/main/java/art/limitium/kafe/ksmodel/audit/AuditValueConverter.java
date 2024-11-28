package art.limitium.kafe.ksmodel.audit;


import art.limitium.kafe.ksmodel.store.WrappedConverter;
import art.limitium.kafe.ksmodel.store.WrappedValueConverter;

public abstract class AuditValueConverter<V> extends WrappedValueConverter<Audit, V> {
    @Override
    protected WrappedConverter<Audit> getWrappedConverter() {
        return new AuditConverter();
    }
}
