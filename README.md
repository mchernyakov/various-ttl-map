# Various time to live cache

## Description

Tiny library with various ttl cache. Based on Redis expire [algorithm](https://redis.io/commands/expire).

The implementation contains two maps: 
1) keys and values,
2) keys and ttl.

And has two variants of cleaning:
1) passive via _get(K)_,
2) active via _BackgroundCleaner_.

### Install

##### Maven Central

_TODO_

## Usage
### Properties

 `defaultTtl` - default ttl (seconds), 
 
 `cleaningPoolSize` - cleaning pool size (default = 1),
 
 `numCleaningAttemptsPerSession` - how many attempts cleaner can do in single session,
 
 `waterMarkPercent` - percent when the cleaner have to start another session 
 (basically it means that we have a lot of expired keys, see [algo](https://redis.io/commands/expire#how-redis-expires-keys)),
 
 `delayMillis`- interval between cleaning sessions (millis, default = 1000).

Code:
```java
        VariousTtlCache<String, String> map = VariousTtlCacheImpl.Builder.newBuilder()
                .setDefaultTtl(2)
                .setCleaningPoolSize(2)
                .setNumCleaningAttemptsPerSession(250)
                .setWaterMarkPercent(10)
                .setDelayMillis(100)
                .build();

        int attempts = 10_000;
        Random random = new Random();
        for (int i = 0; i < attempts; i++) {
            map.put("key_" + random.nextInt(), "val", random.nextInt(5));
        }
```

## Roadmap
- [ ] maven central,
- [ ] options for primitive map for ttl (several engines).