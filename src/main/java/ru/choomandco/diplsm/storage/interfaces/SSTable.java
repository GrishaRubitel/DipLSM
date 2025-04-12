package ru.choomandco.diplsm.storage.interfaces;

import java.util.Map;

public interface SSTable {
    void write(Map<String, String> memTable, String filename);
    String get(String key, String filename);
}
