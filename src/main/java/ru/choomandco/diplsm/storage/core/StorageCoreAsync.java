package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Асинхронная реализация LSM-хранилища.
 * Расширяет {@link StorageCore}, выполняя операции flush и компактацию в отдельных потоках с использованием ExecutorService.
 */
public class StorageCoreAsync extends StorageCore{
    /** Исполнитель задач, связанных с дисковыми операциями */
    protected final ExecutorService diskExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "LSM-Disk-Worker");
        t.setDaemon(true);
        return t;
    });

    /**
     * Конструктор по умолчанию с размером MemTable 4 МБ и порогом компактации 5.
     */
    public StorageCoreAsync() {
        this(4L * 1024 * 1024, 5);
    }

    /**
     * Конструктор с пользовательским размером MemTable.
     * @param memTableMaxSize максимальный размер MemTable в байтах
     */
    public StorageCoreAsync(Long memTableMaxSize) {
        this(memTableMaxSize, 5);
    }

    /**
     * Основной конструктор, вызывает родительский {@link StorageCore}.
     * @param memTableMaxSize максимальный размер MemTable
     * @param sstableTierThreshlod порог количества файлов на уровень до компактации
     */
    public StorageCoreAsync(Long memTableMaxSize, int sstableTierThreshlod) {
        super(memTableMaxSize, sstableTierThreshlod);
    }

    /**
     * Асинхронно сбрасывает MemTable в SSTable.
     * Снимок данных делается сразу, а запись и обновление метаданных происходит в фоне.
     * @param tier уровень, на который выполняется flush
     */
    @Override
    public synchronized void flush(int tier) {
        if (memoryTable.isEmpty()) return;

        Map<String,String> snapshot = new TreeMap<>(memoryTable.getMap());
        String temp = generateNewTableName(tier) + ".temp";
        String finalName = temp.replace(".temp", "");
        memoryTable.emptyMap();

        diskExecutor.submit(() -> {
            //System.out.println("[disk] Starting flush tier=" + tier);
            try {
                table.writeTableFromMap(snapshot, temp);
                if (!new File(temp).renameTo(new File(finalName))) {
                    throw new RuntimeException("rename failed");
                }

                manifestHandler.addNewFile(finalName, tier, MANIFEST_PATH);
                SSTableMetadata meta = new SSTableMetadata(finalName, tier, snapshot.keySet());
                metadataMap.get(tier).add(meta);

                //System.out.println("[disk] Flush complete, scheduling compaction check");
                checkForCompactation(LEVEL_ZERO);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    /**
     * Проверяет, требуется ли компактация, и при необходимости запускает её в фоне.
     * @param level уровень, для которого проверяется необходимость компактации
     */
    @Override
    public void checkForCompactation(int level) {
        if (metadataMap.get(level).size() >= tierThreshold) {
            compactationInitialization(level);
        }
    }

    /**
     * Выполняет компактацию файлов заданного уровня в следующий уровень.
     * Запускается асинхронно. При необходимости рекурсивно вызывает компактацию следующего уровня.
     * @param level уровень, с которого начинается компактация
     */
    @Override
    public void compactationInitialization(int level) {
        List<SSTableMetadata> toCompact = metadataMap.get(level).stream()
                .sorted()
                .limit(tierThreshold)
                .collect(Collectors.toList());
        if (toCompact.isEmpty()) {
            return;
        }

        metadataMap.get(level).removeAll(toCompact);

        int nextLevel = Math.min(level + 1, NUM_OF_LEVELS - 1);
        SSTableMetadata newMeta = compactationEngine.compact(
                toCompact,
                generateNewTableName(nextLevel),
                nextLevel
        );

        metadataMap.get(newMeta.getTier()).add(newMeta);
        manifestHandler.postCompactationRebuild(toCompact, newMeta, MANIFEST_PATH);

        if (metadataMap.get(nextLevel).size() >= tierThreshold) {
            checkForCompactation(nextLevel);
        }
    }
}
