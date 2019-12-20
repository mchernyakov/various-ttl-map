package com.github.mchernyakov.variousttlcache;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VariousTtlMapImplTest {

    @Test(expected = Exception.class)
    public void conditionTest() throws Exception {
        Map<String, Long> map = new HashMap<>();
        map.put("key", 10L);
        boolean res = (System.nanoTime() - map.get("key_not_valid")) > 0L;
        System.out.println(res);
    }
}