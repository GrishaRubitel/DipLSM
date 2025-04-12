package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.interfaces.SSTable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Класс SSTable
 */
public class SortedStringTable implements SSTable {

    /**
     * Метод пишет мапу MemTable в новый SSTable.
     * @param memTableMap Мапа с данными из MemTable
     * @param filename Название файла SSTable
     */
    @Override
    public void writeTableFromMap(Map<String, String> memTableMap, String filename) {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            Map<String, Long> index = new TreeMap<>();

            for (Map.Entry<String, String> entry : memTableMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                long offset = file.getFilePointer();
                index.put(key, offset);

                file.writeByte(key.length());
                file.writeBytes(key);
                file.writeByte(value.length());
                file.writeBytes(value);
            }

            long indexOffset = file.getFilePointer();
            for (Map.Entry<String, Long> entry : index.entrySet()) {
                String key = entry.getKey();
                long offset = entry.getValue();

                file.writeByte(key.length());
                file.writeBytes(key);
                file.writeLong(offset);
            }

            file.writeLong(indexOffset);

        } catch (IOException e) {
            throw new RuntimeException("Failed to writeTableFromMap SSTable", e);
        }
    }

    /**
     * Метод для чтения из SSTable по ключу
     * @param key Ключ
     * @return Значение по ключу
     */
    @Override
    public String getByKey(String key, String filename) {
        return "jopa";
    }

    /**
     * Метод для чтения всего SSTable в мапу
     * @param filename Название файла SSTable
     * @return Мапа данных из SSTable
     */
    @Override
    public Map<String, String> readWholeIntoMap(String filename) {
        Map<String, String> result = new TreeMap<>();

        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {

            file.seek(file.length() - 8);
            long indexOffset = file.readLong();

            file.seek(indexOffset);
            Map<String, Long> offsets = new TreeMap<>();
            while (file.getFilePointer() < file.length() - 8) {
                int keyLen = file.readByte();
                byte[] keyBytes = new byte[keyLen];
                file.readFully(keyBytes);
                String key = new String(keyBytes);

                long dataOffset = file.readLong();
                offsets.put(key, dataOffset);
            }

            for (Map.Entry<String, Long> entry : offsets.entrySet()) {
                file.seek(entry.getValue());

                int kLen = file.readByte();
                byte[] kBytes = new byte[kLen];
                file.readFully(kBytes);
                String key = new String(kBytes);

                int vLen = file.readByte();
                byte[] vBytes = new byte[vLen];
                file.readFully(vBytes);
                String value = new String(vBytes);

                result.put(key, value);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error while reading full SSTable into map", e);
        }

        return result;
    }

    /**
     * Метод для чтения всех строк SSTable в список строк
     * @param filename Название файла SSTable
     * @return Список строк файла SSTable
     */
    @Override
    public List<String> readStringsIntoList(String filename) {
        List<String> result = new ArrayList<>();

        try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
            file.seek(file.length() - 8);
            long indexOffset = file.readLong();

            file.seek(indexOffset);
            Map<String, Long> offsets = new TreeMap<>();
            while (file.getFilePointer() < file.length() - 8) {
                int keyLen = file.readByte();
                byte[] keyBytes = new byte[keyLen];
                file.readFully(keyBytes);
                String key = new String(keyBytes);

                long offset = file.readLong();
                offsets.put(key, offset);
            }

            for (Map.Entry<String, Long> entry : offsets.entrySet()) {
                file.seek(entry.getValue());

                int kLen = file.readByte();
                byte[] kBytes = new byte[kLen];
                file.readFully(kBytes);
                String key = new String(kBytes);

                int vLen = file.readByte();
                byte[] vBytes = new byte[vLen];
                file.readFully(vBytes);
                String value = new String(vBytes);

                result.add(key + "=" + value);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading SSTable as lines", e);
        }

        return result;
    }
}
