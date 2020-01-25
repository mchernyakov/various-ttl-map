package com.github.mchernyakov.variousttlmap.applied.cleaner;

import com.github.mchernyakov.variousttlmap.VariousTtlMap;
import com.github.mchernyakov.variousttlmap.VariousTtlMapImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class AbstractBackgroundMapCleanerTest {

    private static final int POOL_SIZE = 1;

    private VariousTtlMapImpl<String, String> ttlMap;
    private AbstractBackgroundMapCleaner<String, String> mapCleaner;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        ttlMap = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(POOL_SIZE)
                .setNumCleaningAttemptsPerSession(20)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        mapCleaner = (AbstractBackgroundMapCleaner<String, String>) ttlMap.getMapCleaner();
    }

    @After
    public void tearDown() throws Exception {
        ttlMap.clear();
        ttlMap.shutdown();
    }

    @Test
    public void basicTest2() throws Exception {
        VariousTtlMap<String, String> mapClassic = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(2)
                .setNumCleaningAttemptsPerSession(250)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        int attempts = 10_000;
        Random random = new Random();
        for (int i = 0; i < attempts; i++) {
            mapClassic.put("key_" + random.nextInt(), "val", random.nextInt(5));
        }

        await()
                .atMost(4000, MILLISECONDS)
                .until(() -> mapClassic.size() < attempts / 2);
        System.out.println(mapClassic.size());
        mapClassic.shutdown();
    }

    @Test
    public void checkExcessWaterMarkTest() {
        boolean res = mapCleaner.checkExcessWaterMark(100, 25);
        Assert.assertTrue(res);
        res = mapCleaner.checkExcessWaterMark(100, 5);
        Assert.assertFalse(res);
    }

    @Test
    public void checkChunkCreation() throws Exception {
        final int poolSize = 3;
        VariousTtlMapImpl<String, String> map = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(poolSize)
                .setNumCleaningAttemptsPerSession(250)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        MultiThreadMapCleaner<String, String> backgroundMapCleaner = (MultiThreadMapCleaner<String, String>) map.getMapCleaner();

        Set<String> set = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            set.add(String.valueOf(i));
        }

        List<List<String>> res = backgroundMapCleaner.buildChunks(set);
        System.out.println(res);

        assertEquals(res.get(0), Arrays.asList("0", "3", "6", "9"));
        assertEquals(res.get(1), Arrays.asList("1", "4", "7"));
        assertEquals(res.get(2), Arrays.asList("2", "5", "8"));
    }

    @Test
    public void checkChunkCreation0() throws Exception {
        final int poolSize = 2;
        VariousTtlMapImpl<String, String> map = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(poolSize)
                .setNumCleaningAttemptsPerSession(250)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        MultiThreadMapCleaner<String, String> backgroundMapCleaner = (MultiThreadMapCleaner<String, String>) map.getMapCleaner();

        Set<String> set = new HashSet<>();
        for (int i = 0; i < 11; i++) {
            set.add(String.valueOf(i));
        }

        List<List<String>> res = backgroundMapCleaner.buildChunks(set);
        System.out.println(res);

        assertEquals(res.get(0), Arrays.asList("0", "2", "4", "6", "8", "10"));
        assertEquals(res.get(1), Arrays.asList("1", "3", "5", "7", "9"));
    }
}