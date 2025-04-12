package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;
import ru.choomandco.diplsm.storage.interfaces.MemoryTable;
import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;
import ru.choomandco.diplsm.storage.memtable.MemTable;
import ru.choomandco.diplsm.storage.sstable.SSTable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class StorageCore implements DipLSMStorage {
    private final String SSTABLE_FOLDER = "./data/lsm/tables/";
    private final String SSTABLE_MANIFEST_PATH = "./data/lsm/MANIFEST";

    private MemoryTable memoryTable;
    private ManifestHandler manifesthandler;
    private List<SortedStringTable> sortedStringTableList;

    public StorageCore() {
        this(4L * 1024 * 1024);
    }

    public StorageCore(Long memTableMaxSize) {
        this.memoryTable = new MemTable(memTableMaxSize);
        this.manifesthandler = new ManifestHandler();
        this.sortedStringTableList = new LinkedList<>();
        generateTableFolder();
        manifesthandler.readManifest(SSTABLE_MANIFEST_PATH);
    }

    @Override
    public void put(String key, String value) {
         if (memoryTable.put(key, value)) {
            flush();
         }
    }

    @Override
    public String get(String key) {
        return memoryTable.get(key);
    }

    @Override
    public void delete(String key) {
        memoryTable.delete(key);
    }

    @Override
    public void flush() {
        SortedStringTable newTable = new SSTable();
        newTable.writeTableFromMap(memoryTable.getMap(), generateNewTableName());

        sortedStringTableList.add(newTable);
    }

    private void generateTableFolder() {
        try {
            Files.createDirectories(Paths.get(SSTABLE_FOLDER));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateNewTableName() {
        String unixTime = String.valueOf(System.currentTimeMillis() / 1000L);
        return SSTABLE_FOLDER + "sstable_" + unixTime + ".dat";
    }
}
