package ru.choomandco.diplsm.storage.memtable;

import ru.choomandco.diplsm.storage.interfaces.MTEventListener;
import ru.choomandco.diplsm.storage.interfaces.MemTable;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Класс MemTable с кастомизируемым объемом памяти
 */
public class MemoryTable implements MemTable {
    /** Мапа для хранения всех ключ-значений */
    private ConcurrentSkipListMap<String, String> table;
    /** Максимальный размер MemTable в байтах */
    private final long maxSizeInBytes;
    /** Нынешний размер MemTable */
    private long currentSizeInBytes;

    /**
     * Конструктор MemTable по умолчанию, с максимальным размером памяти в 4МБ.
     * Внутри этот метод вызывает кастомный конструктор с предзаписанным размером.
     */
    public MemoryTable() {
        this(4 * 1024 * 1024);
    }

    /**
     * Кастомный конструктор с изменяемым размером памяти
     * @param maxSizeInBytes Максимальный размер MemTable
     */
    public MemoryTable(long maxSizeInBytes) {
        this.table = new ConcurrentSkipListMap<>();
        this.maxSizeInBytes = maxSizeInBytes;
        this.currentSizeInBytes = 0;
    }

    /**
     * Метод, чтобы класть новую пару ключ-значение в MemTabel
     *
     * @param key   Ключ
     * @param value Значение
     * @return
     */
    @Override
    public synchronized boolean put(String key, String value) {
        int keySize = key.getBytes(StandardCharsets.UTF_8).length;
        int valueSize = value.getBytes(StandardCharsets.UTF_8).length;
        int entrySize = keySize + valueSize;

        String oldValue = table.put(key, value);
        if (oldValue != null) {
            currentSizeInBytes -= oldValue.getBytes(StandardCharsets.UTF_8).length;
        }
        currentSizeInBytes += entrySize;

        return isFull();
    }

    /**
     * Метод для извлечения значения по ключу
     * @param key Ключ для извлечения
     * @return Значение ключа
     */
    @Override
    public synchronized String get(String key) {
        return table.getOrDefault(key, null);
    }

    /**
     * Удаляет значение по ключу
     * @param key Ключ значения для удаления
     */
    @Override
    public synchronized void delete(String key) {
        String removedValue = table.remove(key);
        if (removedValue != null) {
            currentSizeInBytes -= (key.getBytes(StandardCharsets.UTF_8).length +
                    removedValue.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    /**
     * Возвращает копию мапы MemTable
     * @return Копия мапы
     */
    @Override
    public synchronized Map<String, String> getEntries() {
        return new TreeMap<String, String>(table);
    }

    /**
     * Метод проверяет заполненность MemTable
     * @return Значение выше лимита или нет (true или false)
     */
    @Override
    public boolean isFull() {
        return currentSizeInBytes >= maxSizeInBytes;
    }

    /**
     * Метод очистки мапы
     */
    @Override
    public void emptyMap() {
        table.clear();
    }
}
