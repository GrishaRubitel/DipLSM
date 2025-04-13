package ru.choomandco.diplsm.storage.interfaces;

import java.util.Map;

public interface MemoryTable {
    boolean put(String key, String value);
    void delete(String key);
    String get(String key);
    Map<String, String> getMap();
    boolean isFull();
    void emptyMap();
    boolean isEmpty();
}
