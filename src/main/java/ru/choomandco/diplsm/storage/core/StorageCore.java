package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;
import ru.choomandco.diplsm.storage.interfaces.MemoryTable;
import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;
import ru.choomandco.diplsm.storage.memtable.MemTable;
import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageCore implements DipLSMStorage {
    private final String SSTABLE_FOLDER = "./data/lsm/tables/";
    private final String SSTABLE_MANIFEST_PATH = "./data/lsm/MANIFEST";
    private final int LEVEL_ZERO = 0;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private MemoryTable memoryTable;
    private ManifestHandler manifestHandler;
    private Map<Integer, TreeSet<SSTableMetadata>> metadataMap;

    public StorageCore() {
        this(4L * 1024 * 1024);
    }

    public StorageCore(Long memTableMaxSize) {
        generateTableFolder();

        this.memoryTable = new MemTable(memTableMaxSize);

        this.manifestHandler = new ManifestHandler();
        manifestHandler.readManifest(SSTABLE_MANIFEST_PATH);

        this.metadataMap = new HashMap<>();
        for (int lvl = 1; lvl <= 3; lvl++) {
            this.metadataMap.put(lvl, new TreeSet<>());
        }

        startFlushTimer();
    }

    @Override
    public void put(String key, String value) {
        lock.readLock().lock();
        try {
            if (memoryTable.put(key, value)) {
                lock.writeLock().lock();
                lock.readLock().unlock();
                try {
                    flush(LEVEL_ZERO);
                } finally {
                    lock.readLock().lock();
                    lock.writeLock().unlock();
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        lock.readLock().lock();
        try {
            String memTableValue = memoryTable.get(key);
            if (memTableValue != null) {
                return memTableValue;
            } else {
                for (int level : new TreeSet<>(metadataMap.keySet())) {
                    TreeSet<SSTableMetadata> tablesTree = metadataMap.get(level);

                    for (SSTableMetadata meta : tablesTree) {
                        if (meta.getBloomFilter().mightContain(key)) {
                            SSTable table = new SSTable();
                            return table.getByKey(key, meta.getFilename());
                        }
                    }
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(String key) {
        lock.readLock().lock();
        try {
            memoryTable.delete(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush(int level) {
        lock.writeLock().lock();
        try {
            SortedStringTable newTable = new SSTable();
            String filename = generateNewTableName();
            newTable.writeTableFromMap(memoryTable.getMap(), filename);

            SSTableMetadata meta = new SSTableMetadata(filename, level, memoryTable.getMap().keySet());

            TreeSet<SSTableMetadata> levelSet = metadataMap.get(level);
            if (levelSet != null) {
                levelSet.add(meta);
            } else {
                throw new IllegalArgumentException("Level " + level + " is not initialized in metadataMap.");
            }

            memoryTable.emptyMap();
        } finally {
            lock.writeLock().unlock();
        }
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

    private List<SSTableMetadata> buildMetadataList() {
        List<SSTableMetadata> newMetadataList = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : manifestHandler.getFileLevels().entrySet()) {
            SortedStringTable table = new SSTable();
            Map<String, String> tableMap = table.readWholeIntoMap(entry.getKey());

            SSTableMetadata meta = new SSTableMetadata(entry.getKey(), entry.getValue(), tableMap.keySet());
            newMetadataList.add(meta);
        }

        return newMetadataList;
    }

    private void startFlushTimer() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(180000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                lock.readLock().lock();
                try {
                    if (!memoryTable.isEmpty()) {
                        lock.readLock().unlock();
                        lock.writeLock().lock();
                        try {
                            flush(LEVEL_ZERO);
                        } finally {
                            lock.writeLock().unlock();
                        }
                    } else {
                        lock.readLock().unlock();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }
}
