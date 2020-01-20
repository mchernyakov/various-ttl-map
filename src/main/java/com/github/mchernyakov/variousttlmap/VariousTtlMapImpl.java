package com.github.mchernyakov.variousttlmap;

import com.github.mchernyakov.variousttlmap.applied.PrimitiveMapWrapper;
import com.github.mchernyakov.variousttlmap.util.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The Map with various ttl for keys.
 * <p>
 * This implementation contains 2 maps :
 * 1) store (key + value) {@link VariousTtlMapImpl#store},
 * 2) map for ttl (key + ttl (when keys will be expired)) {@link VariousTtlMapImpl#ttlMap}.
 * <p>
 * This implementation has two variants of cleaning:
 * 1) passive via {@link VariousTtlMapImpl#get(Object)},
 * 2) active via {@link BackgroundMapCleaner}.
 *
 * @param <K> key
 * @param <V> value
 */
public class VariousTtlMapImpl<K, V> implements VariousTtlMap<K, V> {

    private final ConcurrentHashMap<K, V> store;
    private final PrimitiveMapWrapper ttlMap;
    private final BackgroundMapCleaner<K, V> mapCleaner;

    private final long defaultTtl;
    private final TimeUnit timeUnit = TimeUnit.SECONDS;

    private VariousTtlMapImpl(Builder<K, V> builder) {
        Preconditions.checkArgument(builder.defaultTtl > 0);

        defaultTtl = timeUnit.toNanos(builder.defaultTtl);
        store = new ConcurrentHashMap<>();
        ttlMap = new PrimitiveMapWrapper();

        mapCleaner = BackgroundMapCleaner.Builder
                .newBuilder()
                .setPoolSize(builder.cleaningPoolSize)
                .setDelayTime(builder.delayMillis)
                .setNumKeyCheck(builder.numCleaningAttemptsPerSession)
                .setPercentWaterMark(builder.waterMarkPercent)
                .build(this);

        mapCleaner.startCleaners();
    }

    @Override
    @Nullable
    public V get(@NotNull K key) {
        if (checkExpired(key)) {
            remove(key);
            return null;
        } else {
            return this.store.get(key);
        }
    }

    @Override
    public V put(@NotNull K key, V value) {
        ttlMap.put(key.hashCode(), System.nanoTime() + defaultTtl);
        return store.put(key, value);
    }

    @Override
    public V put(@NotNull K key, V value, long ttlSeconds) {
        ttlMap.put(key.hashCode(), System.nanoTime() + timeUnit.toNanos(ttlSeconds));
        return store.put(key, value);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public V remove(@NotNull K key) {
        ttlMap.remove(key.hashCode());
        return store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
        ttlMap.clear();
    }

    public boolean checkExpired(@NotNull K key) {
        long ttl = ttlMap.get(key.hashCode());

        return System.nanoTime() > ttl;
    }

    Map<K, V> getStore() {
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
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public String toString() {
        return "VariousTtlMapImpl{" +
                "store=" + store +
                ", ttlMap=" + ttlMap +
                ", mapCleaner=" + mapCleaner +
                ", defaultTtl=" + defaultTtl +
                ", timeUnit=" + timeUnit +
                '}';
    }

    public static final class Builder<K, V> {
        long defaultTtl;
        int cleaningPoolSize = 1;
        int numCleaningAttemptsPerSession = 10;
        int waterMarkPercent = 10;
        int delayMillis = 1000;

        private Builder() {
        }

        public static Builder<Object, Object> newBuilder() {
            return new Builder<>();
        }

        public Builder<K, V> setDefaultTtl(long defaultTtl) {
            this.defaultTtl = defaultTtl;
            return this;
        }

        public Builder<K, V> setCleaningPoolSize(int cleaningPoolSize) {
            this.cleaningPoolSize = cleaningPoolSize;
            return this;
        }

        public Builder<K, V> setNumCleaningAttemptsPerSession(int numCleaningAttemptsPerSession) {
            this.numCleaningAttemptsPerSession = numCleaningAttemptsPerSession;
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
        public <K1 extends K, V1 extends V> VariousTtlMapImpl<K1, V1> build() {
            Builder<K1, V1> self = (Builder<K1, V1>) this;
            return new VariousTtlMapImpl<>(self);
        }
    }
}
