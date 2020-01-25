package com.github.mchernyakov.variousttlmap.applied.cleaner;

import com.github.mchernyakov.variousttlmap.VariousTtlMapImpl;
import com.github.mchernyakov.variousttlmap.util.Preconditions;
import com.github.mchernyakov.variousttlmap.util.ThreadUtil;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Map cleaner.
 * <p>
 * Algorithm of active cleaning:
 * Start cleaning workers via {@link AbstractBackgroundMapCleaner#startCleaners()} and {@link AbstractBackgroundMapCleaner#task()} .
 * Work duration is {@link AbstractBackgroundMapCleaner#delayTime}.
 * <p>
 * Inside {@link AbstractBackgroundMapCleaner#task()} we get a array of keys, then check size {@link AbstractBackgroundMapCleaner#numKeyCheck} of keys.
 * <p>
 * Using {@link AbstractBackgroundMapCleaner#checkRandomKey(List)} we check keys.
 * And if percent of deleted keys greater then {@link AbstractBackgroundMapCleaner#percentWaterMark} then we calculate one more time .
 *
 * @param <K>
 * @param <V>
 */
abstract class AbstractBackgroundMapCleaner<K, V> implements BackgroundMapCleaner<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackgroundMapCleaner.class);

    private static final int HUNDRED_PERCENT = 100;
    private static final int RED_LINE_PERCENT = 90;

    private final long delayTime;
    private final int numKeyCheck;
    private final int percentWaterMark;
    protected final int poolSize;

    protected final VariousTtlMapImpl<K, V> map;
    protected final ScheduledExecutorService executorService;

    public AbstractBackgroundMapCleaner(VariousTtlMapImpl<K, V> variousTtlMap, BackgroundMapCleaner.Builder<K, V> builder) {
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

    @Override
    public void startCleaners() {
        for (int i = 0; i < poolSize; i++) {
            executorService.scheduleAtFixedRate(task(), 0, delayTime, TimeUnit.MILLISECONDS);
        }
    }

    private Runnable task() {
        return () -> {
            try {
                additionalInit();

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
                additionalFinally();
            }
        };
    }

    abstract void additionalInit();

    abstract void additionalFinally();

    abstract List<K> getKeys();

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

    @Override
    public void shutdown() {
        ThreadUtil.shutdownExecutorService(executorService);
    }
}
