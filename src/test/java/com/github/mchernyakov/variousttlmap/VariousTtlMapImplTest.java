package com.github.mchernyakov.variousttlmap;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;

public class VariousTtlMapImplTest {

    private VariousTtlMapImpl<String, String> ttlMap;

    @Before
    public void setUp() throws Exception {
        ttlMap = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(1)
                .setNumCleaningAttemptsPerSession(20)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();
    }

    @Test
    public void basicTest0() throws Exception {
        ttlMap.put("one", "1", 1);
        ttlMap.put("two", "2", 1);
        ttlMap.put("three", "3", 1);

        await()
                .atMost(3000, MILLISECONDS)
                .until(() -> ttlMap.isEmpty());
    }

    @Test
    public void basicTest1() throws Exception {
        ttlMap.put("one", "1", 1);
        ttlMap.put("two", "2", 3);
        ttlMap.put("three", "3", 1);
        ttlMap.put("four", "4", 1);
        ttlMap.put("five", "5", 3);
        ttlMap.put("six", "6", 1);

        await()
                .atMost(2200, MILLISECONDS)
                .until(() -> ttlMap.size() > 1 && ttlMap.size() <= 3);
        System.out.println(ttlMap.size());
    }

    @Ignore
    @Test(expected = Exception.class)
    public void conditionTest() throws Exception {
        Map<String, Long> map = new HashMap<>();
        map.put("key", 10L);
        boolean res = (System.nanoTime() - map.get("key_not_valid")) > 0L;
        System.out.println(res);
    }
}