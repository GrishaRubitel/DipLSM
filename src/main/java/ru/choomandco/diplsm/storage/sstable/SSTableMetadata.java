package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.bloomfilter.BloomFilter;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public class SSTableMetadata implements Comparable<SSTableMetadata> {
    private final String filename;
    private int level;
    private String minKey;
    private String maxKey;
    private BloomFilter<String> bloomFilter;

    public SSTableMetadata(String filename, int level, Set<String> keySet) {
        this.filename = filename;
        this.level = level;
        this.minKey = Collections.min(keySet);
        this.maxKey = Collections.max(keySet);

        Function<String, Integer> hash1 = String::hashCode;
        Function<String, Integer> hash2 = s -> s.hashCode() * 31;

        this.bloomFilter = new BloomFilter<String>(1024, new Function[]{ hash1, hash2 });

        bloomFilter.addKeysFromMap(keySet);
    }

    @Override
    public int compareTo(SSTableMetadata meta) {
        return this.filename.compareTo(meta.filename);
    }

    public String getFilename() {
        return filename;
    }

    public int getLevel() {
        return level;
    }

    public String getMinKey() {
        return minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public BloomFilter<String> getBloomFilter() {
        return bloomFilter;
    }

    public void increaseLevel() {
        level++;
    }

    public void decreaseLevel() {
        level--;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }

    public void setBloomFilter(BloomFilter<String> bloomFilter) {
        this.bloomFilter = bloomFilter;
    }
}
