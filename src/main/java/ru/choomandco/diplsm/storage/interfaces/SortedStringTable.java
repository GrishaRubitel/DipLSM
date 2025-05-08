package ru.choomandco.diplsm.storage.interfaces;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface SortedStringTable {
    void writeTableFromMap(Map<String, String> memTable, String filename);
    String getByKey(String key, String filename);
    Map<String, String> readWholeIntoMap(String filename);
    List<String> readStringsIntoList(String filename);
    void deleteFIle(String filename) throws IOException;
}
