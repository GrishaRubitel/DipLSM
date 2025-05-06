package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.util.ArrayList;
import java.util.List;

public class CompactationEngine {

    //TODO переписать
    public void compact(List<SSTableMetadata> tablesMeta) {
        if (tablesMeta.isEmpty()) {
            throw new IllegalArgumentException("No SSTables provided for compaction");
        }
        List<String> allEntries = new ArrayList<>();
        for (SSTableMetadata meta : tablesMeta) {
            allEntries.addAll(new SSTable().readStringsIntoList(meta.getFilename()));
        }
    }
}