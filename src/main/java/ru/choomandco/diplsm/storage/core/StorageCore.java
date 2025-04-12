package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;
import ru.choomandco.diplsm.storage.interfaces.MTEventListener;
import ru.choomandco.diplsm.storage.interfaces.MemTable;
import ru.choomandco.diplsm.storage.interfaces.SSTable;
import ru.choomandco.diplsm.storage.memtable.MemoryTable;
import ru.choomandco.diplsm.storage.sstable.SortedStringTable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class StorageCore implements DipLSMStorage {
    private final String SSTABLE_FOLDER = "./data/lsm/tables/";
    private final String SSTABLE_MANIFEST_PATH = "./data/lsm/MANIFEST";

    private MemTable memTable;
    private ManifestHandler manifesthandler;
    private List<SSTable> ssTableList;

    public StorageCore() {
        this(4L * 1024 * 1024);
    }

    public StorageCore(Long memTableMaxSize) {
        this.memTable = new MemoryTable(memTableMaxSize);
        this.manifesthandler = new ManifestHandler();
        this.ssTableList = new LinkedList<>();
        generateTableFolder();
        manifesthandler.readManifest(SSTABLE_MANIFEST_PATH);
    }

    @Override
    public void put(String key, String value) {
         if (memTable.put(key, value)) {
            flush();
         }
    }

    @Override
    public String get(String key) {
        return memTable.get(key);
    }

    @Override
    public void delete(String key) {
        memTable.delete(key);
    }

    @Override
    public void flush() {

    }

    private void generateTableFolder() {
        try {
            Files.createDirectories(Paths.get(SSTABLE_FOLDER));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
