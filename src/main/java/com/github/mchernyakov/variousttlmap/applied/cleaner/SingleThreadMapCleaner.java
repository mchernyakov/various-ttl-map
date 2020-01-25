package com.github.mchernyakov.variousttlmap.applied.cleaner;

import com.github.mchernyakov.variousttlmap.VariousTtlMapImpl;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

public class SingleThreadMapCleaner<K, V> extends AbstractBackgroundMapCleaner<K, V> {

    public SingleThreadMapCleaner(VariousTtlMapImpl<K, V> variousTtlMap, Builder<K, V> builder) {
        super(variousTtlMap, builder);
    }

    @Override
    void additionalInit() {
        // no-op
    }

    @Override
    void additionalFinally() {
        // no-op
    }

    @VisibleForTesting
    protected List<K> getKeys() {
        //TODO expensive operation
        return new ArrayList<>(map.getStore().keySet());
    }
}
