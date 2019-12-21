package com.github.mchernyakov.variousttlcache;

import com.github.mchernyakov.variousttlcache.util.ThreadUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Map cleaner.
 * <p>
 * Algorithm of active cleaning:
 * Start cleaning workers via {@link BackgroundMapCleaner#startCleaners()} and {@link BackgroundMapCleaner#task()} .
 * Work duration is {@link BackgroundMapCleaner#delayTime}.
 * <p>
 * Inside {@link BackgroundMapCleaner#task()} we get a array of keys, then check size {@link BackgroundMapCleaner#numKeyCheck} of keys.
 * <p>
 * Using {@link BackgroundMapCleaner#checkRandomKey(List)} we check keys.
 * And if percent of deleted keys greater then {@link BackgroundMapCleaner#percentWaterMark} then we calculate one more time .
 *
 * @param <K>
 * @param <V>
 */
class BackgroundMapCleaner<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(BackgroundMapCleaner.class);

    private static final int HUNDRED_PERCENT = 100;
    private static final int RED_LINE_PERCENT = 90;

    private final ScheduledExecutorService executorService;
    private final VariousTtlCacheImpl<K, V> map;

    private final long delayTime;
    private final int poolSize;
    private final int numKeyCheck;
    private final int percentWaterMark;

    private BackgroundMapCleaner(VariousTtlCacheImpl<K, V> variousTtlMap, Builder<K, V> builder) {
        Preconditions.checkArgument(builder.poolSize > 0);
        Preconditions.checkArgument(builder.numKeyCheck > 0);
        Preconditions.checkArgument(builder.delayTime > 0);

        Preconditions.checkArgument(builder.percentWaterMark > 0);
        Preconditions.checkArgument(builder.percentWaterMark < 100);

        delayTime = builder.delayTime;
        numKeyCheck = builder.numKeyCheck;
        poolSize = builder.poolSize;
        percentWaterMark = builder.percentWaterMark;

        map = variousTtlMap;

        ThreadFactory factory = ThreadUtil.threadFactory("map-cleaner");
        executorService = Executors.newScheduledThreadPool(poolSize, factory);
    }

    public void startCleaners() {
        for (int i = 0; i < poolSize; i++) {
            executorService.scheduleAtFixedRate(task(), 0, delayTime, TimeUnit.MILLISECONDS);
        }
    }

    private Runnable task() {
        return () -> {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Start clean");
                }

                // get a array of keys
                List<K> keysAsArray = new ArrayList<>(map.getStore().keySet());
                int size = keysAsArray.size();
                if (size == 0) {
                    return;
                }

                // check and delete
                int numRemovedKeys;
                do {
                    numRemovedKeys = tryRemoveKeys(keysAsArray);
                } while (checkExcessWaterMark(size, numRemovedKeys));

                if (logger.isDebugEnabled()) {
                    logger.debug("Finish clean. num done {}, start size {}", numRemovedKeys, size);
                }
            } catch (Exception e) {
                logger.warn("Error while cleaning map", e);
                throw new RuntimeException(e);
            }
        };
    }

    public boolean checkExcessWaterMark(int size, int numDone) {
        double percent = (HUNDRED_PERCENT * numDone) / (float) size;
        return percent > percentWaterMark && percent < RED_LINE_PERCENT;
    }

    private int tryRemoveKeys(List<K> keys) {
        int numAttempt = 0;
        int numRemovedKeys = 0;
        while (processCondition(numAttempt, keys.size())) {
            if (checkRandomKey(keys)) {
                numRemovedKeys++;
            }
            numAttempt++;
        }
        return numRemovedKeys;
    }

    private boolean processCondition(int numAttempt, int arraySize) {
        return arraySize != 0 && numAttempt < numKeyCheck && numAttempt < arraySize;
    }

    private boolean checkRandomKey(List<K> keys) {
        int num = getRandomIndex(keys.size());
        K key = keys.get(num);
        if (map.checkExpired(key)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Remove key: {}", key);
            }

            map.remove(key);
            return true;
        }
        return false;
    }

    private int getRandomIndex(int size) {
        if (size == 1) {
            return 0;
        }

        return ThreadLocalRandom.current().nextInt(size - 1);
    }

    public void shutdown() {
        ThreadUtil.shutdownExecutorService(executorService);
    }

    static final class Builder<K, V> {
        long delayTime;
        int poolSize;
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
        BackgroundMapCleaner<K1, V1> build(@Nonnull VariousTtlCacheImpl<K1, V1> map) {
            Preconditions.checkNotNull(map);
            Builder<K1, V1> self = (Builder<K1, V1>) this;
            return new BackgroundMapCleaner<>(map, self);
        }
    }
}
