package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Класс SSTable
 */
public class SSTable implements SortedStringTable {

    /**
     * Метод пишет мапу MemTable в новый SSTable.
     * @param memTableMap Мапа с данными из MemTable
     * @param filename Название файла SSTable
     */
    @Override
    public void writeTableFromMap(Map<String, String> memTableMap, String filename) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (Map.Entry<String, String> entry : memTableMap.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue());
                writer.newLine();
            }
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
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2 && parts[0].equals(key)) {
                    return parts[1];
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading SSTable: " + filename, e);
        }
        return null;
    }

    /**
     * Метод для чтения всего SSTable в мапу
     * @param filename Название файла SSTable
     * @return Мапа данных из SSTable
     */
    @Override
    public Map<String, String> readWholeIntoMap(String filename) {
        Map<String, String> result = new TreeMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    result.put(parts[0], parts[1]);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while reading SSTable into map: " + filename, e);
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

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(line.replace(":", "="));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading SSTable lines from file: " + filename, e);
        }

        return result;
    }
}
