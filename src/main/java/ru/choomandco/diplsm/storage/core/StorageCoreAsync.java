package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class StorageCoreAsync extends StorageCore{
    public StorageCoreAsync() {
        this(4L * 1024 * 1024, 5);
    }

    public StorageCoreAsync(Long memTableMaxSize) {
        this(memTableMaxSize, 5);
    }

    public StorageCoreAsync(Long memTableMaxSize, int sstableTierThreshlod) {
        super(memTableMaxSize, sstableTierThreshlod);
    }

    @Override
    public synchronized void flush(int tier) {
        if (memoryTable.isEmpty()) {
            return;
        }

        Map<String, String> snapshot = new TreeMap<>(memoryTable.getMap());
        String tempFilename = generateNewTableName(tier) + ".temp";
        String finalFilename = tempFilename.replace(".temp", "");

        memoryTable.emptyMap();

        flushExecutor.submit(() -> {
            try {
                new SSTable().writeTableFromMap(snapshot, tempFilename);

                File tempFile = new File(tempFilename);
                File finalFile = new File(finalFilename);
                if (!tempFile.renameTo(finalFile)) {
                    throw new RuntimeException("Failed to rename SSTable temp file to final file");
                }

                manifestHandler.addNewFile(finalFilename, tier, MANIFEST_PATH);

                SSTableMetadata meta = new SSTableMetadata(finalFilename, tier, snapshot.keySet());
                metadataMap.computeIfAbsent(tier, k -> new TreeSet<>()).add(meta);
                checkForCompactation(LEVEL_ZERO);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
