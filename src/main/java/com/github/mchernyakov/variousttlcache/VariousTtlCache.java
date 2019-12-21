package com.github.mchernyakov.variousttlcache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface VariousTtlCache<K, V> {

    @Nullable
    V get(K key);

    V put(@Nonnull K key, V value);

    V put(@Nonnull K key, V value, long ttl);

    V remove(@Nonnull K key);

    void clear();

    int size();

    void shutdown();
}
