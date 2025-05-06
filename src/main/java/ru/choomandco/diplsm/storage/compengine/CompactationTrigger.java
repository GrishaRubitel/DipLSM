package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.util.List;
import java.util.ArrayList;

public class CompactationTrigger {

    private final CompactationEngine engine;
    /**
     * Максимальное число SSTable, после которого запускается компактация
     */
    private final int threshold;

    public CompactationTrigger(int threshold) {
        this.threshold = threshold;
        this.engine = new CompactationEngine();
    }

    /**
     * Проверяет, нужно ли запускать компактацию, и запускает её при необходимости.
     */
    public void checkAndCompact(List<SSTableMetadata> sstables) {
        if (sstables.size() >= threshold) {
            engine.compact(sstables);
        }
    }

    /**
     * Принудительный вызов компактации.
     */
    public void forceCompact(List<SSTableMetadata> sstables) {
        if (!sstables.isEmpty()) {
            engine.compact(new ArrayList<>(sstables));
        }
    }
}