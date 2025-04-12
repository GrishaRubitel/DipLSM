package ru.choomandco.diplsm.storage.interfaces;

public interface DipLSMStorage {
    void put(String key, String value);
    String get(String key);
    void delete(String key);
    void flush();
}
