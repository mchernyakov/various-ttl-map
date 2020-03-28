package com.github.mchernyakov.variousttlmap.applied;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PrimitiveMapWrapper {

    private final int numBuckets;
    private final Int2LongOpenHashMap[] primitiveMaps;
    private final ReadWriteLock[] rwLocks;

    public PrimitiveMapWrapper() {
        this(16);
    }

    public PrimitiveMapWrapper(int buckets) {
        numBuckets = buckets;
        primitiveMaps = new Int2LongOpenHashMap[buckets];
        rwLocks = new ReentrantReadWriteLock[buckets];
        for (int i = 0; i < buckets; i++) {
            primitiveMaps[i] = new Int2LongOpenHashMap();
            rwLocks[i] = new ReentrantReadWriteLock();
        }
    }

    private int getBucketId(int hash) {
        return Math.abs(hash % numBuckets);
    }

    public void put(int hash, long value) {
        int bucketId = getBucketId(hash);
        Lock lock = rwLocks[bucketId].writeLock();
        lock.lock();
        try {
            primitiveMaps[bucketId].put(hash, value);
        } finally {
            lock.unlock();
        }
    }

    public void remove(int hash) {
        int bucketId = getBucketId(hash);
        Lock lock = rwLocks[bucketId].writeLock();
        lock.lock();
        try {
            primitiveMaps[bucketId].remove(hash);
        } finally {
            lock.unlock();
        }
    }

    public long get(int hash) {
        int bucketId = getBucketId(hash);
        Lock lock = rwLocks[bucketId].readLock();
        lock.lock();
        try {
            return primitiveMaps[bucketId].get(hash);
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        for (int i = 0; i < primitiveMaps.length; i++) {
            clearMap(i);
        }
    }

    private void clearMap(int i) {
        Int2LongOpenHashMap map = primitiveMaps[i];
        if (map != null) {
            Lock lock = rwLocks[i].writeLock();
            lock.lock();
            try {
                map.clear();
            } finally {
                lock.unlock();
            }
        }
    }
}
