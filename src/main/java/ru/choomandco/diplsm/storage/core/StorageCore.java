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
    private final int LEVEL_ZERO = 0;
    private static final AtomicLong FILE_COUNTER = new AtomicLong();

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
        manifestHandler.readManifest(SSTABLE_FOLDER);

        this.metadataMap = new ConcurrentSkipListMap<>();
        for (int lvl = 0; lvl <= 2; lvl++) {
            this.metadataMap.put(lvl, new TreeSet<>());
        }

        for (Map.Entry<String, Integer> entry : manifestHandler.getFileTiers().entrySet()) {
            SSTableMetadata meta = new SSTableMetadata(entry.getKey(), entry.getValue(), new SSTable().readWholeIntoMap(entry.getKey()).keySet());
            metadataMap.get(entry.getValue()).add(meta);
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
    public synchronized void flush(int tier) {
        if (memoryTable.isEmpty()) return;

        SortedStringTable newTable = new SSTable();
        Map<String, String> snapshot = new TreeMap<>(memoryTable.getMap());

        String tempFilename = generateNewTableName(tier) + ".temp";
        newTable.writeTableFromMap(snapshot, tempFilename);

        String finalFilename = tempFilename.replace(".temp", "");
        File tempFile = new File(tempFilename);
        File finalFile = new File(finalFilename);
        if (!tempFile.renameTo(finalFile)) {
            throw new RuntimeException("Failed to rename SSTable temp file to final file");
        }

        manifestHandler.addNewFile(finalFilename, tier);
        SSTableMetadata meta = new SSTableMetadata(finalFilename, tier, snapshot.keySet());
        metadataMap.computeIfAbsent(tier, k -> new TreeSet<>()).add(meta);

        memoryTable.emptyMap();
    }

    private void generateTableFolder() {
        try {
            Files.createDirectories(Paths.get(SSTABLE_FOLDER));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "T0"));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "T1"));
            Files.createDirectories(Paths.get(SSTABLE_FOLDER + "T2"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateNewTableName(int tier) {
        String timestamp = System.currentTimeMillis() + "_" + FILE_COUNTER.incrementAndGet();
        return SSTABLE_FOLDER + "T" + tier + "/sstable_" + timestamp + ".dat";
    }

    private List<SSTableMetadata> buildMetadataList() {
        List<SSTableMetadata> newMetadataList = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : manifestHandler.getFileTiers().entrySet()) {
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
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }
}
