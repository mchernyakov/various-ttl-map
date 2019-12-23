package com.github.mchernyakov.variousttlmap.applied;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PrimitiveMapWrapper {

    private final Int2LongOpenHashMap primitiveMap;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public PrimitiveMapWrapper() {
        primitiveMap = new Int2LongOpenHashMap();
    }

    public void put(int hash, long value) {
        writeLock.lock();
        try {
            primitiveMap.put(hash, value);
        } finally {
            writeLock.unlock();
        }
    }

    public void remove(int hash) {
        writeLock.lock();
        try {
            primitiveMap.remove(hash);
        } finally {
            writeLock.unlock();
        }
    }

    public long get(int hash) {
        readLock.lock();
        try {
            return primitiveMap.get(hash);
        } finally {
            readLock.unlock();
        }
    }

    public void clear() {
        primitiveMap.clear();
    }
}
