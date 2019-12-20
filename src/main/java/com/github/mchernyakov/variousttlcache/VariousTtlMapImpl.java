package com.github.mchernyakov.variousttlcache;

import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Мапа с разными ttl на ключи.
 * <p>
 * Содержит 3 мапы: ключ+значение, ключ+timestamp (когда записали), ключ+ttl.
 * 2 варианта очистки: пассивный через запрос {@link VariousTtlMapImpl#get(Object)}
 * и активный через {@link BackgroundMapCleaner}.
 * <p>
 * Более подробное описание очистки в {@link BackgroundMapCleaner}
 *
 * @param <K> ключ
 * @param <V> значение
 */
public class VariousTtlMapImpl<K, V> implements VariousTtlMap<K, V> {

    private final ConcurrentHashMap<K, V> store;
    private final ConcurrentHashMap<K, Long> timestamps;
    private final ConcurrentHashMap<K, Long> ttlMap;
    private final BackgroundMapCleaner<K, V> mapCleaner;

    private final long defaultTtl;
    private final TimeUnit timeUnit = TimeUnit.SECONDS;

    private VariousTtlMapImpl(Builder<K, V> builder) {
        Preconditions.checkArgument(builder.defaultTtl > 0);

        defaultTtl = timeUnit.toNanos(builder.defaultTtl);
        store = new ConcurrentHashMap<>();
        ttlMap = new ConcurrentHashMap<>();
        timestamps = new ConcurrentHashMap<>();

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
    public V put(K key, V value) {
        timestamps.put(key, System.nanoTime());
        ttlMap.put(key, defaultTtl);
        return store.put(key, value);
    }

    @Override
    public V put(K key, V value, long ttl) {
        timestamps.put(key, System.nanoTime());
        ttlMap.put(key, timeUnit.toNanos(ttl));
        return store.put(key, value);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public V remove(K key) {
        timestamps.remove(key);
        ttlMap.remove(key);
        return store.remove(key);
    }

    @Override
    public void clear() {
        timestamps.clear();
        store.clear();
        ttlMap.clear();
    }

    public boolean checkExpired(K key) {
        Long keyTimestamp = timestamps.get(key);
        Long ttl = ttlMap.get(key);

        return keyTimestamp == null || ttl == null || (System.nanoTime() - keyTimestamp) > ttl;
    }

    public ConcurrentHashMap<K, V> getStore() {
        return store;
    }

    public BackgroundMapCleaner<K, V> getMapCleaner() {
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
                ", timestamps=" + timestamps +
                ", ttlMap=" + ttlMap +
                ", mapCleaner=" + mapCleaner +
                ", defaultTtl=" + defaultTtl +
                ", timeUnit=" + timeUnit +
                '}';
    }

    public static final class Builder<K, V> {
        long defaultTtl;
        int clearPoolSize;
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
        public <K1 extends K, V1 extends V> VariousTtlMapImpl<K1, V1> build() {
            Builder<K1, V1> self = (Builder<K1, V1>) this;
            return new VariousTtlMapImpl<>(self);
        }
    }
}
