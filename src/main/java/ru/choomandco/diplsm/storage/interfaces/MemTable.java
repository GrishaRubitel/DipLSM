package ru.choomandco.diplsm.storage.interfaces;

import java.util.Map;

public interface MemTable {
    boolean put(String key, String value);
    void delete(String key);
    String get(String key);
    Map<String, String> getEntries();
    boolean isFull();
    void emptyMap();
}
