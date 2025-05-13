package ru.choomandco.diplsm.storage.interfaces;

import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.util.List;

public interface CompEngine {
    SSTableMetadata compact(List<SSTableMetadata> tablesMeta, String fileToCompact, int level);
}
