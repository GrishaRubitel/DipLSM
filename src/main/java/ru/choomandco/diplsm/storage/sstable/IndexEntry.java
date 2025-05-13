package ru.choomandco.diplsm.storage.sstable;

public class IndexEntry implements Comparable<IndexEntry> {
    private final String key;
    private final long offset;

    public IndexEntry(String key, long offset) {
        this.key = key;
        this.offset = offset;
    }

    @Override
    public int compareTo(IndexEntry other) {
        return this.key.compareTo(other.key);
    }

    public String getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }
}
