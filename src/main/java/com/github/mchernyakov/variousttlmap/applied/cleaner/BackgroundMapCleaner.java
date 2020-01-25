package com.github.mchernyakov.variousttlmap.applied.cleaner;

import com.github.mchernyakov.variousttlmap.VariousTtlMapImpl;
import com.github.mchernyakov.variousttlmap.util.Preconditions;
import org.jetbrains.annotations.NotNull;

public interface BackgroundMapCleaner<K, V> {

    void startCleaners();

    void shutdown();

    final class Builder<K, V> {
        private static final int DEFAULT_POOL_SIZE = 1;

        long delayTime;
        int poolSize = DEFAULT_POOL_SIZE;
        int numKeyCheck;
        int percentWaterMark;

        private Builder() {
        }

        public static Builder<Object, Object> newBuilder() {
            return new Builder<>();
        }

        public Builder<K, V> setDelayTime(long delayTime) {
            this.delayTime = delayTime;
            return this;
        }

        public Builder<K, V> setPoolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public Builder<K, V> setNumKeyCheck(int numKeyCheck) {
            this.numKeyCheck = numKeyCheck;
            return this;
        }

        public Builder<K, V> setPercentWaterMark(int percentWaterMark) {
            this.percentWaterMark = percentWaterMark;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <K1 extends K, V1 extends V>
        BackgroundMapCleaner<K1, V1> build(@NotNull VariousTtlMapImpl<K1, V1> map) {
            Preconditions.checkNotNull(map);
            Builder<K1, V1> self = (Builder<K1, V1>) this;

            if (poolSize > DEFAULT_POOL_SIZE) {
                return new MultiThreadMapCleaner<>(map, self);
            } else {
                return new SingleThreadMapCleaner<>(map, self);
            }
        }
    }
}
