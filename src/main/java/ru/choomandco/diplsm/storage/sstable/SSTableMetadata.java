package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.bloomfilter.BloomFilter;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class SSTableMetadata implements Comparable<SSTableMetadata> {
    private final String filename;
    private int tier;
    private String minKey;
    private String maxKey;
    private BloomFilter<String> bloomFilter;

    public SSTableMetadata(String filename, int tier, Set<String> keySet) {
        this.filename = filename;
        this.tier = tier;
        this.minKey = Collections.min(keySet);
        this.maxKey = Collections.max(keySet);

        Function<String, Integer> hash1 = String::hashCode;
        Function<String, Integer> hash2 = s -> s.hashCode() * 31;

        this.bloomFilter = new BloomFilter<String>(1024, new Function[]{ hash1, hash2 });

        bloomFilter.addKeysFromMap(keySet);
    }

    @Override
    public int compareTo(SSTableMetadata other) {
        if (Objects.equals(this.filename, other.getFilename())) {
            return 0;
        }

        return Long.compare(extractTimestamp(this.filename), extractTimestamp(other.filename));
    }

    private long extractTimestamp(String filename) {
        String[] parts = filename.split("_");
        return Long.parseLong(parts[1]);
    }

    public String getFilename() {
        return filename;
    }

    public int getTier() {
        return tier;
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

    public void increaseTier() {
        tier++;
    }

    public void decreaseTier() {
        tier--;
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
