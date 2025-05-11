package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class StorageCoreAsync extends StorageCore{
    private final ExecutorService diskExecutor = Executors.newSingleThreadExecutor();

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
        String tempFilename  = generateNewTableName(tier) + ".temp";
        String finalFilename = tempFilename.replace(".temp", "");
        memoryTable.emptyMap();

        diskExecutor.submit(() -> {
            try {
                new SSTable().writeTableFromMap(snapshot, tempFilename);
                if (!new File(tempFilename).renameTo(new File(finalFilename))) {
                    throw new RuntimeException("Failed to rename temp to final SSTable");
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

    @Override
    protected void checkForCompactation(int level) {
        if (metadataMap.get(level).size() >= tierThreshold) {
            diskExecutor.submit(() -> compactationInitialization(level));
        }
    }

    @Override
    protected void compactationInitialization(int level) {
        List<SSTableMetadata> toCompact = metadataMap.get(level).stream()
                .limit(tierThreshold)
                .collect(Collectors.toList());
        if (toCompact.isEmpty()) return;

        metadataMap.get(level).removeAll(toCompact);

        int nextLevel = (level == NUM_OF_LEVELS - 1) ? level : level + 1;
        SSTableMetadata newMeta = compactationEngine.compact(toCompact, generateNewTableName(nextLevel), nextLevel);

        metadataMap
                .computeIfAbsent(newMeta.getTier(), k -> new TreeSet<>())
                .add(newMeta);

        manifestHandler.postCompactationRebuild(toCompact, newMeta, MANIFEST_PATH);

        if (nextLevel < NUM_OF_LEVELS && metadataMap.get(nextLevel).size() >= tierThreshold) {
            checkForCompactation(nextLevel);
        }
    }
}
