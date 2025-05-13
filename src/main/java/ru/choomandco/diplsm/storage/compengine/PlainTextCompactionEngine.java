package ru.choomandco.diplsm.storage.compengine;

import ru.choomandco.diplsm.storage.interfaces.CompEngine;
import ru.choomandco.diplsm.storage.sstable.PlainTextSSTable;

public class PlainTextCompactionEngine extends CompactationEngine implements CompEngine {
    public PlainTextCompactionEngine() {
        table = new PlainTextSSTable();
    }
}
