package com.github.mchernyakov.variousttlmap;

import com.github.mchernyakov.variousttlmap.util.Preconditions;
import com.github.mchernyakov.variousttlmap.util.ThreadUtil;
import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
    private final VariousTtlMapImpl<K, V> map;

    private final long delayTime;
    private final int poolSize;
    private final int numKeyCheck;
    private final int percentWaterMark;
    private final boolean isMultiThreadCleaner;

    private final BlockingQueue<List<K>> blockingQueue;

    private Phaser phaser;
    private volatile boolean isInitChunks;

    private BackgroundMapCleaner(VariousTtlMapImpl<K, V> variousTtlMap, Builder<K, V> builder) {
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
        isMultiThreadCleaner = poolSize > 1;

        blockingQueue = new ArrayBlockingQueue<>(poolSize);
    }

    public void startCleaners() {
        if (isMultiThreadCleaner) {
            phaser = new Phaser();
        }

        for (int i = 0; i < poolSize; i++) {
            executorService.scheduleAtFixedRate(task(), 0, delayTime, TimeUnit.MILLISECONDS);
        }
    }

    private Runnable task() {
        return () -> {
            try {
                if (isMultiThreadCleaner) {
                    phaser.register();
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Start cleaning");
                }

                // get a array of keys
                List<K> keysAsArray;
                while ((keysAsArray = getKeys()) != null) {

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
                }
            } catch (Exception e) {
                logger.warn("Error while cleaning map", e);
                throw new RuntimeException(e);
            } finally {
                if (isMultiThreadCleaner) {
                    phaser.arriveAndDeregister();
                    if (phaser.getArrivedParties() == 0) {
                        isInitChunks = false;
                    }
                }
            }
        };
    }

    @VisibleForTesting
    List<K> getKeys() {
        if (!isMultiThreadCleaner) { // single-thread-pool case
            //TODO expensive operation
            offerChunk(new ArrayList<>(map.getStore().keySet()));
        } else {
            if (!isInitChunks) {
                synchronized (this) {
                    if (!isInitChunks) {
                        initAndPutChunks();
                        isInitChunks = true;
                    }
                }
            }
        }

        return blockingQueue.poll();
    }

    private void initAndPutChunks() {
        Set<K> keys = map.getStore().keySet();
        List<List<K>> chunks = buildChunks(keys);
        chunks.forEach(this::offerChunk);
    }

    @VisibleForTesting
    List<List<K>> buildChunks(Set<K> keys) {
        List<List<K>> chunks = new ArrayList<>();
        IntStream.range(0, poolSize).forEach(i -> chunks.add(new ArrayList<>()));

        int counter = 0;
        for (K key : keys) {
            int index = counter % poolSize;
            chunks.get(index).add(key);
            counter++;
        }

        return chunks;
    }

    private void offerChunk(List<K> chunk) {
        boolean res = blockingQueue.offer(chunk);
        if (!res) {
            throw new IllegalStateException("queue capacity lower than chunk count");
        }
    }

    @VisibleForTesting
    boolean checkExcessWaterMark(int size, int numDone) {
        float percent = (HUNDRED_PERCENT * numDone) / (float) size;
        return percent > percentWaterMark && percent < RED_LINE_PERCENT;
    }

    private int tryRemoveKeys(List<K> keys) {
        int numAttempt = 0;
        int numRemovedKeys = 0;
        int size = keys.size();
        while (processCondition(numAttempt, size)) {
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

    private static int getRandomIndex(int size) {
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
        BackgroundMapCleaner<K1, V1> build(@NotNull VariousTtlMapImpl<K1, V1> map) {
            Preconditions.checkNotNull(map);
            Builder<K1, V1> self = (Builder<K1, V1>) this;
            return new BackgroundMapCleaner<>(map, self);
        }
    }
}
