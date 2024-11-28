package art.limitium.kafe.ksmodel.audit;

import art.limitium.kafe.ksmodel.primitive.PrimitiveNulls;
import art.limitium.kafe.ksmodel.store.WrappedConverter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class AuditConverter implements WrappedConverter<Audit> {

    public static String prefix = "AUDIT__";

    @Override
    public SchemaBuilder fillSchema(SchemaBuilder builder) {
        return builder
                .field(prefix + "TRACE_ID", Schema.INT64_SCHEMA)
                .field(prefix + "VERSION", Schema.INT32_SCHEMA)
                .field(prefix + "PARTITION", Schema.INT32_SCHEMA)
                .field(prefix + "CREATED_AT", Schema.INT64_SCHEMA)
                .field(prefix + "MODIFIED_AT", Schema.INT64_SCHEMA)
                .field(prefix + "MODIFIED_BY", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "REASON", Schema.OPTIONAL_STRING_SCHEMA)
                .field(prefix + "REMOVED", Schema.BOOLEAN_SCHEMA)
                ;
    }

    @Override
    public void fillStruct(Struct struct, Audit audit) {
        struct
                .put(prefix + "TRACE_ID", audit.traceId())
                .put(prefix + "VERSION", audit.version())
                .put(prefix + "PARTITION", audit.partition())
                .put(prefix + "CREATED_AT", audit.createdAt())
                .put(prefix + "MODIFIED_AT", audit.modifiedAt())
                .put(prefix + "MODIFIED_BY", audit.modifiedBy())
                .put(prefix + "REASON", audit.reason())
                .put(prefix + "REMOVED", audit.removed())
        ;
    }

    @Override
    public Audit createObject(Schema schema, Struct struct) {
        return new Audit(
                PrimitiveNulls.primitiveFrom(struct.getInt64(prefix + "TRACE_ID")),
                PrimitiveNulls.primitiveFrom(struct.getInt32(prefix + "VERSION")),
                PrimitiveNulls.primitiveFrom(struct.getInt32(prefix + "PARTITION")),
                PrimitiveNulls.primitiveFrom(struct.getInt64(prefix + "CREATED_AT")),
                PrimitiveNulls.primitiveFrom(struct.getInt64(prefix + "MODIFIED_AT")),
                struct.getString(prefix + "MODIFIED_BY"),
                struct.getString(prefix + "REASON"),
                struct.getBoolean(prefix + "REMOVED")
        );
    }

    @Override
    public Serde<Audit> getSerde() {
        return Audit.AuditSerde();
    }
}
