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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class StorageCore implements DipLSMStorage {
    private final String SSTABLE_FOLDER = "./data/lsm/tables/";
    private final String SSTABLE_MANIFEST_PATH = "./data/lsm/MANIFEST";
    private final int LEVEL_ZERO = 0;

    private MemoryTable memoryTable;
    private ManifestHandler manifestHandler;
    private Map<Integer, TreeSet<SSTableMetadata>> metadataMap;
    private static final AtomicLong FILE_COUNTER = new AtomicLong();

    public StorageCore() {
        this(4L * 1024 * 1024);
    }

    public StorageCore(Long memTableMaxSize) {
        generateTableFolder();

        this.memoryTable = new MemTable(memTableMaxSize);

        this.manifestHandler = new ManifestHandler();
        manifestHandler.readManifest(SSTABLE_MANIFEST_PATH);

        this.metadataMap = new ConcurrentSkipListMap<>();
        for (int lvl = 0; lvl <= 2; lvl++) {
            this.metadataMap.put(lvl, new TreeSet<>());
        }

        startFlushTimer();
    }

    @Override
    public void put(String key, String value) {
        if (memoryTable.put(key, value)) {
            flush(LEVEL_ZERO);
        }
    }

    @Override
    public String get(String key) {
        String memTableValue = memoryTable.get(key);
        if (memTableValue != null) {
            return memTableValue;
        }

        for (int level : new TreeSet<>(metadataMap.keySet())) {
            for (SSTableMetadata meta : metadataMap.get(level)) {
                if (meta.getBloomFilter().mightContain(key)) {
                    return new SSTable().getByKey(key, meta.getFilename());
                }
            }
        }

        return null;
    }

    @Override
    public void delete(String key) {
        memoryTable.delete(key);
    }

    @Override
    public synchronized void flush(int level) {
        SortedStringTable newTable = new SSTable();
        Map<String, String> snapshot = new TreeMap<>(memoryTable.getMap()); // создаём копию!

        String filename = generateNewTableName(level);
        newTable.writeTableFromMap(snapshot, filename);

        SSTableMetadata meta = new SSTableMetadata(filename, level, snapshot.keySet());
        metadataMap.computeIfAbsent(level, k -> new TreeSet<>()).add(meta);

        memoryTable.emptyMap();
    }

    private void generateTableFolder() {
        try {
            Files.createDirectories(Paths.get(SSTABLE_FOLDER));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "L0"));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "L1"));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "L2"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateNewTableName(int level) {
        String timestamp = System.currentTimeMillis() + "_" + FILE_COUNTER.incrementAndGet();
        return SSTABLE_FOLDER + "L" + level + "/sstable_" + timestamp + ".dat";
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
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(180000); // 3 минуты = 180000

                    if (!memoryTable.isEmpty()) {
                        flush(LEVEL_ZERO);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // корректно выходим
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }
}
