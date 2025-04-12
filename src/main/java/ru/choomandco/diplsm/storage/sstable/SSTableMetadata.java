package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.bloomfilter.BloomFilter;

public class SSTableMetadata {
    private final String filename;
    private int level;
    private String minKey;
    private String maxKey;
    private BloomFilter<String> bloomFilter;

    public SSTableMetadata(String filename, int level, String minKey, String maxKey, BloomFilter<String> bloomFilter) {
        this.filename = filename;
        this.level = level;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.bloomFilter = bloomFilter;
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
