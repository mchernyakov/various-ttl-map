# Map with various time to live of keys

[![Build Status](https://travis-ci.com/mchernyakov/various-ttl-map.svg?branch=master)](https://travis-ci.com/mchernyakov/various-ttl-map)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.mchernyakov/various-ttl-map/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.mchernyakov/various-ttl-map/)

## Description

I's a cache (map) with various ttl of keys based on Redis [expire algorithm](https://redis.io/commands/expire)
and ConcurrentHashMap.

The implementation contains two maps:

* keys and values,
* keys and ttl.

And has two cleaning modes:

* `passive` via _get(K)_,
* `active` via _BackgroundCleaner_.

The BackgroundCleaner contains a thread pool, which is responsible for cleaning the map.

## Install

#### Maven

```xml

<dependency>
    <groupId>com.github.mchernyakov</groupId>
    <artifactId>various-ttl-map</artifactId>
    <version>0.0.3</version>
</dependency>
```

#### Gradle

```groovy
compile group: 'com.github.mchernyakov', name: 'various-ttl-map', version: '0.0.3'
```

## Usage

### Properties

Builder properties:

`defaultTtl` - default ttl (seconds),

`cleaningPoolSize` - cleaning pool size (default = 1),

`numCleaningAttemptsPerSession` - how many attempts cleaner can do in single session,

`waterMarkPercent` - percent when the cleaner has to start another session
(basically, it means that we have a lot of expired keys,
see [algo](https://redis.io/commands/expire#how-redis-expires-keys)),

`delayMillis`- interval between cleaning sessions (millis, default = 1000).

#### In code

```java
    VariousTtlMap<String, String> map=VariousTtlMapImpl.Builder.newBuilder()
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

- [ ] size of the cache,
- [ ] options for primitive map for ttl (several engines),
- [ ] async API,
- [ ] jmh tests.