package com.github.mchernyakov.variousttlcache;

public interface VariousTtlMap<K, V> {

    V get(K key);

    V put(K key, V value);

    V put(K key, V value, long ttl);

    V remove(K key);

    void clear();

    int size();

    void shutdown();
}
