package com.github.mchernyakov.variousttlcache;

import com.github.mchernyakov.variousttlcache.util.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The Cache with various ttl for keys.
 * <p>
 * This implementation contains 3 maps :
 * 1) store (key + value) {@link VariousTtlCacheImpl#store},
 * 2) map for ttl (key + ttl (when keys will be expired)) {@link VariousTtlCacheImpl#ttlMap}.
 * <p>
 * This implementation has two variants of cleaning:
 * 1) passive via {@link VariousTtlCacheImpl#get(K)},
 * 2) active via {@link BackgroundMapCleaner}.
 *
 * @param <K> key
 * @param <V> value
 */
public class VariousTtlCacheImpl<K, V> implements VariousTtlCache<K, V> {

    private final ConcurrentHashMap<K, V> store;
    private final ConcurrentHashMap<K, Long> ttlMap;
    private final BackgroundMapCleaner<K, V> mapCleaner;

    private final long defaultTtl;
    private final TimeUnit timeUnit = TimeUnit.SECONDS;

    private VariousTtlCacheImpl(Builder<K, V> builder) {
        Preconditions.checkArgument(builder.defaultTtl > 0);

        defaultTtl = timeUnit.toNanos(builder.defaultTtl);
        store = new ConcurrentHashMap<>();
        ttlMap = new ConcurrentHashMap<>();

        mapCleaner = BackgroundMapCleaner.Builder
                .newBuilder()
                .setPoolSize(builder.clearPoolSize)
                .setDelayTime(builder.delayMillis)
                .setNumKeyCheck(builder.numCheck)
                .setPercentWaterMark(builder.waterMarkPercent)
                .build(this);

        mapCleaner.startCleaners();
    }

    @Override
    @Nullable
    public V get(K key) {
        V value = this.store.get(key);

        if (value != null && checkExpired(key)) {
            remove(key);
            return null;
        } else {
            return value;
        }
    }

    @Override
    public V put(@NotNull K key, V value) {
        ttlMap.put(key, System.nanoTime() + defaultTtl);
        return store.put(key, value);
    }

    @Override
    public V put(@NotNull K key, V value, long ttl) {
        ttlMap.put(key, System.nanoTime() + timeUnit.toNanos(ttl));
        return store.put(key, value);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public V remove(@NotNull K key) {
        ttlMap.remove(key);
        return store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
        ttlMap.clear();
    }

    public boolean checkExpired(@NotNull K key) {
        Long ttl = ttlMap.get(key);

        return ttl == null || System.nanoTime() > ttl;
    }

    ConcurrentHashMap<K, V> getStore() {
        return store;
    }

    BackgroundMapCleaner<K, V> getMapCleaner() {
        return mapCleaner;
    }

    @Override
    public void shutdown() {
        mapCleaner.shutdown();
        clear();
    }

    @Override
    public String toString() {
        return "VariousTtlMapClassic{" +
                "store=" + store +
                ", ttlMap=" + ttlMap +
                ", mapCleaner=" + mapCleaner +
                ", defaultTtl=" + defaultTtl +
                ", timeUnit=" + timeUnit +
                '}';
    }

    public static final class Builder<K, V> {
        long defaultTtl;
        int clearPoolSize = 1;
        int numCheck;
        int waterMarkPercent;
        int delayMillis;

        private Builder() {
        }

        public static Builder<Object, Object> newBuilder() {
            return new Builder<>();
        }

        public Builder<K, V> setDefaultTtl(long defaultTtl) {
            this.defaultTtl = defaultTtl;
            return this;
        }

        public Builder<K, V> setClearPoolSize(int clearPoolSize) {
            this.clearPoolSize = clearPoolSize;
            return this;
        }

        public Builder<K, V> setNumCheck(int numCheck) {
            this.numCheck = numCheck;
            return this;
        }

        public Builder<K, V> setWaterMarkPercent(int waterMarkPercent) {
            this.waterMarkPercent = waterMarkPercent;
            return this;
        }

        public Builder<K, V> setDelayMillis(int delayMillis) {
            this.delayMillis = delayMillis;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <K1 extends K, V1 extends V> VariousTtlCacheImpl<K1, V1> build() {
            Builder<K1, V1> self = (Builder<K1, V1>) this;
            return new VariousTtlCacheImpl<>(self);
        }
    }
}
