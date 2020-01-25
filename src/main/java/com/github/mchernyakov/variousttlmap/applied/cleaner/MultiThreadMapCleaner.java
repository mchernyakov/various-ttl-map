package com.github.mchernyakov.variousttlmap.applied.cleaner;

import com.github.mchernyakov.variousttlmap.VariousTtlMapImpl;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

public class MultiThreadMapCleaner<K, V> extends AbstractBackgroundMapCleaner<K, V> {

    private final BlockingQueue<List<K>> blockingQueue;
    private final Phaser phaser;

    private volatile boolean isInitChunks = false;

    public MultiThreadMapCleaner(VariousTtlMapImpl<K, V> variousTtlMap, Builder<K, V> builder) {
        super(variousTtlMap, builder);

        this.phaser = new Phaser();
        this.blockingQueue = new ArrayBlockingQueue<>(this.poolSize);
    }

    @Override
    void additionalInit() {
        this.phaser.register();
    }

    @Override
    void additionalFinally() {
        this.phaser.arriveAndDeregister();
        if (this.phaser.getArrivedParties() == 0) {
            this.isInitChunks = false;
        }
    }

    @Override
    protected List<K> getKeys() {
        if (!this.isInitChunks) {
            synchronized (this) {
                if (!this.isInitChunks) {
                    initAndPutChunks();
                    this.isInitChunks = true;
                }
            }
        }

        return this.blockingQueue.poll();
    }

    private void initAndPutChunks() {
        Set<K> keys = this.map.getStore().keySet();
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
        boolean res = this.blockingQueue.offer(chunk);
        if (!res) {
            throw new IllegalStateException("queue capacity lower than chunk count");
        }
    }
}
