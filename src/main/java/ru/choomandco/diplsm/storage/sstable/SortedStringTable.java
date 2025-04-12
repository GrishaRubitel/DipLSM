package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.interfaces.MemTable;
import ru.choomandco.diplsm.storage.interfaces.SSTable;
import ru.choomandco.diplsm.storage.memtable.MemoryTable;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Класс SSTable
 */
public class SortedStringTable implements SSTable {
    /**
     * Метод для перезаписи данных из MemTable в SSTable
     * @param memTable Объект MemTable для перезаписи
     */
    @Override
    public void write(Map<String, String> memTableMap, String filename) {

    }

    /**
     * Метод для чтения из SSTable по ключу
     * @param key Ключ
     * @return Значение по ключу
     */
    @Override
    public String get(String key, String filename) {

        return "jopa";
    }
}
