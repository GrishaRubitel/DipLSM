package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.exception.invalid.crc.InvalidCRC;
import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.CRC32;

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
            CRC32 crc = new CRC32();
            StringBuilder contentBuffer = new StringBuilder();

            for (Map.Entry<String, String> entry : memTableMap.entrySet()) {
                String line = entry.getKey() + ":" + entry.getValue();
                contentBuffer.append(line).append("\n");
            }

            byte[] dataBytes = contentBuffer.toString().getBytes();
            crc.update(dataBytes);

            writer.write("#CRC=" + crc.getValue());
            writer.newLine();

            writer.write(contentBuffer.toString());

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
            verifyCRC(reader, filename);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2 && parts[0].equals(key)) {
                    return parts[1];
                }
            }
        } catch (InvalidCRC e) {
            throw new RuntimeException(e);
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
            verifyCRC(reader, filename);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                if (parts.length == 2) {
                    result.put(parts[0], parts[1]);
                }
            }
        } catch (InvalidCRC e) {
            throw new RuntimeException(e);
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
            verifyCRC(reader, filename);
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(line.replace(":", "="));
            }
        } catch (InvalidCRC e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException("Error reading SSTable lines from file: " + filename, e);
        }

        return result;
    }

    /**
     * Метод для удаления файла SSTable
     * @param filename имя файла для удаления
     * @throws IOException ошибка удаления
     */
    @Override
    public void deleteFIle(String filename) throws IOException {
        Files.delete(Paths.get(filename));
    }

    /**
     * Валидация контрольной суммы файла SSTable
     * @param reader буферизированный "читатель" файлов
     * @param filename название файла SSTable для чтения
     * @throws InvalidCRC ошибка некорректной контрольной суммы
     */
    private void verifyCRC(BufferedReader reader, String filename) throws InvalidCRC {
        try {
            reader.mark(1024 * 1024);

            String crcLine = reader.readLine();
            if (crcLine == null || !crcLine.startsWith("#CRC=")) {
                throw new InvalidCRC("Missing or malformed CRC line in file: " + filename);
            }

            long expectedCrc = Long.parseLong(crcLine.substring(5));
            StringBuilder content = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

            CRC32 crc = new CRC32();
            crc.update(content.toString().getBytes());
            long actualCrc = crc.getValue();

            if (actualCrc != expectedCrc) {
                throw new InvalidCRC("CRC mismatch in file: " + filename +
                        ", expected=" + expectedCrc + ", actual=" + actualCrc);
            }

            reader.reset();

            reader.readLine();

        } catch (IOException e) {
            throw new RuntimeException("Error during CRC verification in file: " + filename, e);
        }
    }
}
