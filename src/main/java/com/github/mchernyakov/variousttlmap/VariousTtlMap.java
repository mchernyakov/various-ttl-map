package com.github.mchernyakov.variousttlmap;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface VariousTtlMap<K, V> {

    @Nullable
    V get(K key);

    V put(@NotNull K key, V value);

    V put(@NotNull K key, V value, long ttl);

    V remove(@NotNull K key);

    void clear();

    int size();

    void shutdown();
}