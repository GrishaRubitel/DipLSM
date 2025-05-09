package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;
import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.IOException;
import java.util.*;

public class CompactationEngine {

    public SSTableMetadata compact(List<SSTableMetadata> tablesMeta, String fileToCompact, int level) {
        SortedStringTable table = new SSTable();

        if (tablesMeta.isEmpty()) {
            throw new IllegalArgumentException("No SSTables provided for compaction");
        }

        Map<String, String> allEntries = new TreeMap<>();
        for (SSTableMetadata meta : tablesMeta) {
            allEntries.putAll(table.readWholeIntoMap(meta.getFilename()));
        }

        table.writeTableFromMap(allEntries, fileToCompact);

        for (SSTableMetadata file : tablesMeta) {
            try {
                table.deleteFIle(file.getFilename());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new SSTableMetadata(fileToCompact, level, allEntries.keySet());
    }
}