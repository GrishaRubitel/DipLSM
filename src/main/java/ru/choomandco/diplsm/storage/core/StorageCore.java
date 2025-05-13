package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.compengine.CompactationEngine;
import ru.choomandco.diplsm.storage.interfaces.CompEngine;
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
import java.util.stream.Collectors;

/**
 * Основной класс реализации LSM-хранилища.
 * Отвечает за работу с MemTable, SSTable, а также управление флашами, компактацией и метаданными.
 */
public class StorageCore implements DipLSMStorage {
    /** Путь к директории, где хранятся SSTable-файлы */
    protected final String SSTABLE_FOLDER = "./data/lsm/tables/";
    /** Путь к файлу MANIFEST, содержащему информацию об уровнях хранения SSTable-файлов */
    protected final String MANIFEST_PATH = "./data/lsm/MANIFEST";
    /** Нулевой уровень в иерархии уровней LSM */
    protected final int LEVEL_ZERO = 0;
    /** Общее количество уровней в LSM */
    protected final int NUM_OF_LEVELS = 5;
    /** Счётчик файлов, используемый для генерации уникальных имён SSTable */
    protected final AtomicLong FILE_COUNTER = new AtomicLong();
    /** Порог количества SSTable-файлов на уровень, после которого запускается компактация */
    protected int tierThreshold;

    /** MemTable — структура в памяти для временного хранения данных */
    protected MemoryTable memoryTable;
    /** Объект для работы с MANIFEST-файлом */
    protected ManifestHandler manifestHandler;
    /** Движок компактации SSTable-файлов */
    protected CompEngine compactationEngine;
    /** Метаданные всех SSTable-файлов, отсортированные по уровням */
    protected Map<Integer, TreeSet<SSTableMetadata>> metadataMap;
    /**
     * Объект для взаимодействия с файлами SSTable
     */
    SortedStringTable table = new SSTable();

    /**
     * Конструктор по умолчанию.
     * Использует размер MemTable по умолчанию и порог компактации = 5.
     */
    public StorageCore() {
        this(1024L * 1024, 5);
    }

    /**
     * Конструктор с пользовательским размером MemTable.
     * @param memTableMaxSize максимальный размер MemTable в байтах
     */
    public StorageCore(Long memTableMaxSize) {
        this(memTableMaxSize, 5);
    }

    /**
     * Основной конструктор, инициализирует хранилище, директории, читает MANIFEST и восстанавливает метаданные.
     * @param memTableMaxSize максимальный размер MemTable
     * @param sstableTierThreshlod порог количества файлов на уровень до компактации
     */
    public StorageCore(Long memTableMaxSize, int sstableTierThreshlod) {
        tierThreshold = sstableTierThreshlod;

        generateTableFolder();

        memoryTable = new MemTable(memTableMaxSize);

        manifestHandler = new ManifestHandler();
        manifestHandler.readManifest(SSTABLE_FOLDER, MANIFEST_PATH);

        metadataMap = new ConcurrentSkipListMap<>();
        for (int lvl = 0; lvl < NUM_OF_LEVELS; lvl++) {
            this.metadataMap.put(lvl, new TreeSet<>());
        }

        for (Map.Entry<String, Integer> entry : manifestHandler.getFileTiers().entrySet()) {
            SSTableMetadata meta = null;
            try {
                meta = new SSTableMetadata(entry.getKey(), entry.getValue(), table.readWholeIntoMap(entry.getKey()).keySet());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            metadataMap.get(entry.getValue()).add(meta);
        }

        compactationEngine = new CompactationEngine();

        for (Map.Entry<Integer, TreeSet<SSTableMetadata>> entry : metadataMap.entrySet()) {
            while (entry.getValue().size() >= tierThreshold) {
                compactationInitialization(entry.getKey());
            }
        }
        startFlushTimer();
    }

    /**
     * Записывает ключ-значение в память. Если MemTable переполнена — вызывается flush.
     */
    @Override
    public void put(String key, String value) {
        if (memoryTable.put(key, value)) {
            flush(LEVEL_ZERO);
        }
    }

    /**
     * Получает значение по ключу, сначала из памяти, затем из SSTable-файлов.
     */
    @Override
    public String get(String key) {
        String memTableValue = memoryTable.get(key);
        if (memTableValue != null) {
            return memTableValue;
        }

        try {
            for (int level : new TreeSet<>(metadataMap.keySet())) {
                TreeSet<SSTableMetadata> levelSet = metadataMap.get(level);
                if (levelSet == null) continue;

                Iterator<SSTableMetadata> descendingIterator = levelSet.descendingIterator();
                while (descendingIterator.hasNext()) {
                    SSTableMetadata meta = descendingIterator.next();
                    if (meta.getBloomFilter().mightContain(key)) {
                        return table.getByKey(key, meta.getFilename());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error while itterating - " + e);
            return null;
        }


        return null;
    }

    /**
     * Удаляет ключ из памяти (логическое удаление).
     */
    @Override
    public void delete(String key) {
        memoryTable.delete(key);
    }

    /**
     * Выполняет флаш MemTable на диск в SSTable-файл, после чего очищает MemTable.
     * Также обновляет MANIFEST и метаданные.
     */
    @Override
    public synchronized void flush(int tier) {
        if (memoryTable.isEmpty()) {
            return;
        }

        Map<String, String> snapshot = new TreeMap<>(memoryTable.getMap());

        String tempFilename = generateNewTableName(tier) + ".temp";
        table.writeTableFromMap(snapshot, tempFilename);

        String finalFilename = tempFilename.replace(".temp", "");
        File tempFile = new File(tempFilename);
        File finalFile = new File(finalFilename);
        if (!tempFile.renameTo(finalFile)) {
            throw new RuntimeException("Failed to rename SSTable temp file to final file");
        }

        manifestHandler.addNewFile(finalFilename, tier, MANIFEST_PATH);
        SSTableMetadata meta = new SSTableMetadata(finalFilename, tier, snapshot.keySet());
        metadataMap.computeIfAbsent(tier, k -> new TreeSet<>()).add(meta);

        memoryTable.emptyMap();

        checkForCompactation(LEVEL_ZERO);
    }

    /**
     * Проверяет, нужно ли запускать компактацию на указанном уровне.
     * При необходимости вызывает рекурсивно на следующий уровень.
     */
    protected void checkForCompactation(int level) {
        if (metadataMap.get(level).size() >= tierThreshold) {
            compactationInitialization(level);
            if (level + 1 != NUM_OF_LEVELS) {
                checkForCompactation(level + 1);
            }
        }
    }

    /**
     * Инициализирует компактацию файлов на заданном уровне.
     * Компактация перемещает данные на следующий уровень и обновляет метаданные.
     */
    protected void compactationInitialization(int level) {
        List<SSTableMetadata> listToCompact = metadataMap.get(level).stream()
                                    //.sorted()
                                    .limit(tierThreshold)
                                    .collect(Collectors.toList());

        int targetLevel = (level == NUM_OF_LEVELS - 1) ? level : level + 1;
        if (listToCompact.isEmpty()) {
            return;
        }
        metadataMap.get(level).removeAll(listToCompact);
        SSTableMetadata newMeta = compactationEngine.compact(new ArrayList<>(listToCompact), generateNewTableName(targetLevel), targetLevel);
        metadataMap.get(newMeta.getTier()).add(newMeta);

        manifestHandler.postCompactationRebuild(listToCompact, newMeta, MANIFEST_PATH);
    }

    //TODO удалить перед релизом
    /**
     * Принудительная компактация нулевого уровня. Используется в отладочных целях.
     */
    public void forceCompact() {
        compactationInitialization(LEVEL_ZERO);
    }

    /**
     * Генерирует уникальное имя нового SSTable-файла с указанием уровня хранения.
     */
    protected String generateNewTableName(int tier) {
        String timestamp = System.currentTimeMillis() + "_" + FILE_COUNTER.incrementAndGet();
        return SSTABLE_FOLDER + "T" + tier + "/sstable_" + timestamp + ".sst";
    }

    /**
     * Запускает таймер для периодического флаша MemTable на диск (каждые 3 минуты).
     */
    protected void startFlushTimer() {
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

    /**
     * Создаёт директории для всех уровней SSTable-хранилища, если они ещё не существуют.
     */
    protected void generateTableFolder() {
        try {
            Files.createDirectories(Paths.get(SSTABLE_FOLDER));
            for (int i = 0; i < NUM_OF_LEVELS; i++) {
                Files.createDirectories(Paths.get(SSTABLE_FOLDER + "T" + i));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
