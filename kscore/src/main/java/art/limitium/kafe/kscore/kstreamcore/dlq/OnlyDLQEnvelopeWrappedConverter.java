package art.limitium.kafe.kscore.kstreamcore.dlq;

import art.limitium.kafe.ksmodel.store.WrappedConverter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class OnlyDLQEnvelopeWrappedConverter extends DLQEnvelopeWrappedValueConverter<Bytes> {

    @Override
    protected WrappedConverter<Bytes> getValueConverter() {
        return new WrappedConverter<>() {
            @Override
            public SchemaBuilder fillSchema(SchemaBuilder builder) {
                return builder;
            }

            @Override
            public void fillStruct(Struct struct, Bytes bytes) {

            }

            @Override
            public Bytes createObject(Schema schema, Struct struct) {
                return null;
            }

            @Override
            public Serde<Bytes> getSerde() {
                return Serdes.Bytes();
            }
        };
    }
}
