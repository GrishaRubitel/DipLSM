package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.interfaces.CompEngine;
import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;
import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.IOException;
import java.util.*;

/**
 * Класс, реализующий движок компактации для LSM-структуры хранения.
 * Объединяет несколько SSTable-файлов в один, устраняя дубликаты ключей
 * и освобождая место за счёт удаления устаревших файлов.
 */
public class CompactationEngine implements CompEngine {
    protected SortedStringTable table ;

    public CompactationEngine() {
        table = new SSTable();
    }

    /**
     * Выполняет компактацию заданного списка SSTable-файлов.
     * Все ключи и значения из указанных таблиц объединяются,
     * при этом в случае дублирования ключей остаётся последнее значение (по порядку в списке).
     * Создаётся новый SSTable, старые файлы удаляются.
     *
     * @param tablesMeta список метаданных SSTable-файлов, подлежащих компактации
     * @param fileToCompact имя нового SSTable-файла, в который будут записаны данные
     * @param level уровень, на который будет записан результат компактации
     * @return метаинформация о новом SSTable-файле
     * @throws IllegalArgumentException если список таблиц пуст
     * @throws RuntimeException если не удалось удалить один из исходных файлов
     */
    public SSTableMetadata compact(List<SSTableMetadata> tablesMeta, String fileToCompact, int level) {
        if (tablesMeta.isEmpty()) {
            throw new IllegalArgumentException("No SSTables provided for compaction");
        }

        Map<String, String> allEntries = new TreeMap<>();
        for (SSTableMetadata meta : tablesMeta) {
            try {
                allEntries.putAll(table.readWholeIntoMap(meta.getFilename()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        table.writeTableFromMap(allEntries, fileToCompact);

        for (SSTableMetadata file : tablesMeta) {
            try {
//                System.out.println("[compact] About to delete: " + file.getFilename() + " (exists=" + Files.exists(Paths.get(file.getFilename())) + ")");
                table.deleteFIle(file.getFilename());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new SSTableMetadata(fileToCompact, level, allEntries.keySet());
    }
}