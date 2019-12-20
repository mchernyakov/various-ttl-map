package com.github.mchernyakov.variousttlcache;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class BackgroundMapCleanerTest {
    private VariousTtlMapImpl<String, String> mapClassic;
    private BackgroundMapCleaner<String, String> mapCleaner;

    @Before
    public void setUp() throws Exception {
        mapClassic = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setClearPoolSize(1)
                .setNumCheck(20)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        mapCleaner = mapClassic.getMapCleaner();
    }

    @After
    public void tearDown() throws Exception {
        mapClassic.shutdown();
    }

    @Test
    public void basicTest0() throws Exception {
        mapClassic.put("one", "1", 1);
        mapClassic.put("two", "2", 1);
        mapClassic.put("three", "3", 1);
        Thread.sleep(3000);
        Assert.assertEquals(0, mapClassic.size());
    }

    @Test
    public void basicTest1() throws Exception {
        mapClassic.put("one", "1", 1);
        mapClassic.put("two", "2", 3);
        mapClassic.put("three", "3", 1);
        mapClassic.put("four", "4", 1);
        mapClassic.put("five", "5", 3);
        mapClassic.put("six", "6", 1);
        Thread.sleep(2000);
        Assert.assertTrue(mapClassic.size() > 1 && mapClassic.size() <= 3);
        System.out.println(mapClassic.size());
    }

    @Test
    public void basicTest2() throws Exception {
        // спецмапа для бОльшего количество ключей за одну проверку (мапа не успевает почиститься)
        VariousTtlMapImpl<String, String> mapClassic = VariousTtlMapImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setClearPoolSize(2)
                .setNumCheck(250)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        int attempts = 10_000;
        Random random = new Random();
        for (int i = 0; i < attempts; i++) {
            mapClassic.put("key_" + random.nextInt(), "val", random.nextInt(5));
        }

        Thread.sleep(4000);
        System.out.println(mapClassic.size());

        // примитивная проверка, что кол-во записей в мапе будет меньше половины попыток (по факту их от 2к до 3к)
        Assert.assertTrue(mapClassic.size() < attempts / 2);
        mapClassic.shutdown();
    }

    @Test
    public void checkExcessWaterMarkTest() {
        boolean res = mapCleaner.checkExcessWaterMark(100, 25);
        Assert.assertTrue(res);
        res = mapCleaner.checkExcessWaterMark(100, 5);
        Assert.assertFalse(res);
    }
}