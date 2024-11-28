package art.limitium.kafe.kscore.kstreamcore.processor;

import art.limitium.kafe.ksmodel.store.WrappedValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.WrappedIndexedKeyValueStore;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.ProcessorPostInitListener;
import org.apache.kafka.streams.state.internals.WrapperSupplierFactoryAware;

import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
class WrappedIndexedKeyValueDecorator<S extends IndexedMeteredKeyValueStore<K, WrappedValue<W, V>>, K, V, W> extends WrappedKeyValueDecorator<S, K, V, W> implements WrappedIndexedKeyValueStore<K, V, W>, ProcessorPostInitListener {

    public WrappedIndexedKeyValueDecorator(S store, WrapperSupplierFactoryAware<K, V, W, ExtendedProcessorContext> wrapperSupplierFactoryAware, ExtendedProcessorContext pc) {
        super(store, wrapperSupplierFactoryAware, pc);
    }

    @Override
    public WrappedValue<W, V> getUnique(String indexName, String indexKey) {
        return getWrapped().getUnique(indexName, indexKey);
    }

    @Override
    public K getUniqKey(String indexName, String indexKey) {
        return getWrapped().getUniqKey(indexName, indexKey);
    }

    @Override
    public Stream<WrappedValue<W, V>> getNonUnique(String indexName, String indexKey) {
        return getWrapped().getNonUnique(indexName, indexKey);
    }


    @Override
    public void onPostInit(ProcessorContext processorContext) {
        getWrapped().onPostInit(processorContext);

    }

    @SuppressWarnings("unchecked")
    private S getWrapped() {
        return (S) wrapped();
    }
}
