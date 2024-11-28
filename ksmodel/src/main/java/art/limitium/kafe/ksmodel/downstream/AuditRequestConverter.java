package art.limitium.kafe.ksmodel.downstream;

import art.limitium.kafe.ksmodel.audit.AuditValueConverter;
import art.limitium.kafe.ksmodel.store.WrappedConverter;

public class AuditRequestConverter extends AuditValueConverter<Request> {

    @Override
    protected WrappedConverter<Request> getValueConverter() {
        return new RequestConverter();
    }
}
