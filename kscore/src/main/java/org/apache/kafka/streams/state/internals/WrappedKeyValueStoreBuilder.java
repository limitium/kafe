package org.apache.kafka.streams.state.internals;

import art.limitium.kafe.ksmodel.store.WrappedValue;
import art.limitium.kafe.ksmodel.store.WrapperValueSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;

import java.util.Objects;

@SuppressWarnings("rawtypes")
public class WrappedKeyValueStoreBuilder<K, V, W, PC extends ProcessorContext> extends AbstractStoreBuilder<K, WrappedValue<W, V>, WrappedKeyValueStore<K, V, W>> {

    private final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory;

    private final KeyValueBytesStoreSupplier storeSupplier;

    public WrappedKeyValueStoreBuilder(KeyValueBytesStoreSupplier storeSupplier, Serde<K> keySerde, Serde<V> valueSerde, Serde<W> wrapperSerde, WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory, Time time) {
        super(storeSupplier.name(), keySerde, new WrapperValueSerde<>(wrapperSerde, valueSerde), time);
        Objects.requireNonNull(storeSupplier, "storeSupplier can't be null");
        Objects.requireNonNull(storeSupplier.metricsScope(), "storeSupplier's metricsScope can't be null");
        Objects.requireNonNull(wrapperSupplierFactory, "wrapperSupplierFactory can't be null");
        this.storeSupplier = storeSupplier;
        this.wrapperSupplierFactory = wrapperSupplierFactory;
    }


    @Override
    public WrappedKeyValueStore<K, V, W> build() {
        return new WrappedMeteredKeyValueStore<>(
                maybeWrapCaching(maybeWrapLogging(storeSupplier.get())),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde,
                wrapperSupplierFactory);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner, false);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingKeyValueBytesStore(inner);
    }
}
