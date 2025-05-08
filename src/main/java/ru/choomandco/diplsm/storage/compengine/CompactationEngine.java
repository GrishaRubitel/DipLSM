package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.IOException;
import java.util.*;

public class CompactationEngine {

    public SSTableMetadata compact(List<SSTableMetadata> tablesMeta, String fileToCompact, int level) {
        if (tablesMeta.isEmpty()) {
            throw new IllegalArgumentException("No SSTables provided for compaction");
        }

        Map<String, String> allEntries = new TreeMap<>();
        for (SSTableMetadata meta : tablesMeta) {
            allEntries.putAll(new SSTable().readWholeIntoMap(meta.getFilename()));
        }

        new SSTable().writeTableFromMap(allEntries, fileToCompact);

        for (SSTableMetadata file : tablesMeta) {
            try {
                new SSTable().deleteFIle(file.getFilename());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new SSTableMetadata(fileToCompact, level, allEntries.keySet());
    }
}