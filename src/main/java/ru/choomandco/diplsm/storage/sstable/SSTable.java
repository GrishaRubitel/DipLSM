package ru.choomandco.diplsm.storage.sstable;

import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Класс SSTable, работающий с бинарным текстом
 */
public class SSTable implements SortedStringTable {
    private static final int FOOTER_SIZE = 12;
    private static final int MAGIC = 0x4C534D31; // 'LSM1'
    /**
     * Метод пишет мапу MemTable в новый SSTable.
     * @param memTableMap Мапа с данными из MemTable
     * @param filename Название файла SSTable
     */
    @Override
    public void writeTableFromMap(Map<String, String> memTableMap, String filename) {
        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            raf.setLength(0);
            raf.seek(12);

            List<IndexEntry> index = new ArrayList<>();
            for (Map.Entry<String,String> e : memTableMap.entrySet()) {
                long pos = raf.getFilePointer();
                byte[] key = e.getKey().getBytes(UTF_8);
                byte[] val = e.getValue().getBytes(UTF_8);

                raf.writeInt(key.length);
                raf.write(key);
                raf.writeInt(val.length);
                raf.write(val);

                index.add(new IndexEntry(e.getKey(), pos));
            }

            long indexOffset = raf.getFilePointer();
            raf.writeInt(index.size());
            for (IndexEntry ie : index) {
                byte[] key = ie.getKey().getBytes(UTF_8);
                raf.writeInt(key.length);
                raf.write(key);
                raf.writeLong(ie.getOffset());
            }

            raf.seek(0);
            raf.writeLong(indexOffset);
            raf.writeInt(0x4C534D31);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Метод для чтения из SSTable по ключу
     * @param key Ключ
     * @return Значение по ключу
     */
    @Override
    public String getByKey(String key, String filename) {
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            raf.seek(0);
            long indexOffset = raf.readLong();
            int magic = raf.readInt();
            if (magic != 0x4C534D31) throw new IOException("Bad SSTable magic");

            raf.seek(indexOffset);
            int count = raf.readInt();

            List<IndexEntry> idx = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int klen = raf.readInt();
                byte[] kbs = new byte[klen]; raf.readFully(kbs);
                long off = raf.readLong();
                idx.add(new IndexEntry(new String(kbs, UTF_8), off));
            }
            int pos = Collections.binarySearch(
                    idx,
                    new IndexEntry(key, -1),
                    Comparator.comparing(IndexEntry::getKey)
            );
            if (pos < 0) return null;
            long dataOff = idx.get(pos).getOffset();

            raf.seek(dataOff);
            int klen = raf.readInt();
            raf.skipBytes(klen);
            int vlen = raf.readInt();
            byte[] vbs = new byte[vlen]; raf.readFully(vbs);
            return new String(vbs, UTF_8);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Читает весь SSTable-файл в отсортированное отображение ключ→значение.
     * Формат бинарного файла:
     * [Data Block][Index Block][Footer]
     * Footer (12 байт) = [indexOffset (8 байт)][magic (4 байта)]
     *
     * @param filename путь к SSTable-файлу
     * @return TreeMap с данными из файла
     * @throws IOException при ошибках I/O или некорректном формате
     */
    @Override
    public Map<String, String> readWholeIntoMap(String filename) throws IOException {
        Map<String,String> result = new TreeMap<>();
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {

            raf.seek(0);
            long indexOffset = raf.readLong();
            int magic = raf.readInt();
            if (magic != 0x4C534D31) throw new IOException("Invalid SSTable file (magic mismatch): " + filename);

            raf.seek(FOOTER_SIZE);
            while (raf.getFilePointer() < indexOffset) {
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String val = new String(valBytes, StandardCharsets.UTF_8);
                result.put(key, val);
            }
        }
        return result;
    }

    /**
     * Читает все строки SSTable-файла и возвращает список строк вида "key=value".
     * Формирует список в том порядке, в котором записаны пары в Data Block.
     *
     * @param filename путь к SSTable-файлу
     * @return List<String> всех записей "key=value"
     * @throws IOException при ошибках I/O или некорректном формате
     */
    @Override
    public List<String> readStringsIntoList(String filename) throws IOException {
        List<String> result = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            long fileSize = raf.length();
            raf.seek(fileSize - FOOTER_SIZE);
            long indexOffset = raf.readLong();
            int magic = raf.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid SSTable file (magic mismatch): " + filename);
            }

            raf.seek(FOOTER_SIZE);
            while (raf.getFilePointer() < indexOffset) {
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);

                int valLen = raf.readInt();
                byte[] valBytes = new byte[valLen];
                raf.readFully(valBytes);

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String val = new String(valBytes, StandardCharsets.UTF_8);
                result.add(key + "=" + val);
            }
        }
        return result;
    }

    /**
     * Удаляет файл SSTable.
     *
     * @param filename путь к файлу
     * @throws IOException при ошибке удаления
     */
    @Override
    public void deleteFIle(String filename) throws IOException {
        Path p = Paths.get(filename).toAbsolutePath().normalize();
        try {
            Files.delete(p);
//            System.out.println("[delete] Deleted SSTable " + p);
        } catch (IOException e) {
//            System.err.println("[delete] FAILED to delete SSTable " + p + ": " + e.getMessage());
            throw e;
        }    }
}
