package org.apache.kafka.streams.state.internals;


import art.limitium.kafe.ksmodel.store.WrappedValue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WrappedIndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrapperSupplier.WrapperSupplierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;


@SuppressWarnings("rawtypes")
public class WrappedIndexedMeteredKeyValueStore<K, V, W, PC extends ProcessorContext> extends IndexedMeteredKeyValueStore<K, WrappedValue<W, V>> implements WrappedIndexedKeyValueStore<K, V, W> {

    private static final Logger logger = LoggerFactory.getLogger(WrappedIndexedMeteredKeyValueStore.class);

    //@todo: shouldn't be part of store, must be moved to some meta holder
    private final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory;
    private WrapperSupplier<K, V, W, ?> wrapperSupplier;

    WrappedIndexedMeteredKeyValueStore(final Map<String, Function<WrappedValue<W, V>, String>> uniqIndexes,
                                       final Map<String, Function<WrappedValue<W, V>, String>> nonUniqIndexes,
                                       final KeyValueStore<Bytes, byte[]> inner,
                                       final String metricsScope,
                                       final Time time,
                                       final Serde<K> keySerde,
                                       final Serde<WrappedValue<W, V>> valueSerde,
                                       final WrapperSupplierFactory<K, V, W, PC> wrapperSupplierFactory) {
        super(uniqIndexes, nonUniqIndexes, inner, metricsScope, time, keySerde, valueSerde);
        this.wrapperSupplierFactory = wrapperSupplierFactory;
    }

    @Override
    public W getWrapper(K key) {
        WrappedValue<W, V> wrappedValue = get(key);
        if (wrappedValue == null) {
            return null;
        }
        return wrappedValue.wrapper();
    }

    @Override
    public V getValue(K key) {
        WrappedValue<W, V> wrappedValue = get(key);
        if (wrappedValue == null) {
            return null;
        }
        return wrappedValue.value();
    }

    @Override
    public void putValue(K key, V value) {
        put(key, new WrappedValue<>(wrapperSupplier.generate(key, value), value));
    }


    @Override
    @SuppressWarnings("unchecked")
    public void onPostInit(ProcessorContext processorContext) {
        super.onPostInit(processorContext);
        this.wrapperSupplier = wrapperSupplierFactory.create(this, (PC) processorContext);
    }
}
