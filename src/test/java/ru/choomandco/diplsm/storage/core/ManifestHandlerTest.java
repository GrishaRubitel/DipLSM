package ru.choomandco.diplsm.storage.core;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.choomandco.diplsm.storage.interfaces.SortedStringTable;
import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ManifestHandlerTest {
    private static final String TEST_DIR = ".\\data_test\\lsm\\tables\\";
    private static final String MANIFEST_PATH = "./data_test/lsm/MANIFEST";
    private ManifestHandler handler;

    @BeforeEach
    void construct() throws IOException {
        Path baseDir = Paths.get(TEST_DIR);
        if (Files.exists(baseDir)) {
            FileUtils.deleteDirectory(baseDir.toFile());
        }
        Files.createDirectories(baseDir.resolve("T1"));
        Files.createDirectories(baseDir.resolve("T2"));

        handler = new ManifestHandler();
    }

    @AfterEach
    void deconstruct() throws IOException {
        FileUtils.deleteDirectory(new File(".\\data_test"));
        Files.deleteIfExists(Paths.get(MANIFEST_PATH));
    }

    @Test
    void testRebuildManifestCreatesEntries() throws IOException {
        String file1 = TEST_DIR + "T1\\a.dat";
        String file2 = TEST_DIR + "T2\\b.dat";

        Files.write(Paths.get(file1), "");
        Files.write(Paths.get(file2), "");

        handler.readManifest(TEST_DIR, MANIFEST_PATH);
        Map<String, Integer> tiers = handler.getFileTiers();
        assertTrue(tiers.get(file1).equals(1));
        assertTrue(tiers.get(file2).equals(2));
    }

    @Test
    void testLoadAndVerifyManifest() throws IOException {
        String dummyName = TEST_DIR + "T1\\test_file.dat";
        Map<String, String> dummyMap = new HashMap<>();
        dummyMap.put("ke1", "val1");

        SortedStringTable dummyTable = new SSTable();
        dummyTable.writeTableFromMap(dummyMap, dummyName);

        handler.readManifest(TEST_DIR, MANIFEST_PATH);
        dummyTable.deleteFIle(dummyName);
        assertTrue(handler.getFileTiers().containsKey(dummyName));
    }

    @Test
    void testUpdateAndAddNewFile() {
        handler.addNewFile("c.dat", 1, MANIFEST_PATH);
        assertEquals(1, handler.getFileTiers().get("c.dat"));

        handler.updateFileTier("c.dat", 2, MANIFEST_PATH);
        assertEquals(2, handler.getFileTiers().get("c.dat"));
    }

    @Test
    void testPostCompactionRebuild() {
        Map<String, String> dummyMap = new HashMap<>();
        dummyMap.put("key1", "val1");
        dummyMap.put("key2", "val2");
        Set<String> dummySet = dummyMap.keySet();

        SSTableMetadata old1 = new SSTableMetadata("./data/lsm/tables/Ttest/old1.dat", 1, dummySet);
        SSTableMetadata old2 = new SSTableMetadata("./data/lsm/tables/Ttest/old2.dat", 1, dummySet);
        SSTableMetadata newFile = new SSTableMetadata("./data/lsm/tables/Ttest/new.dat", 2, dummySet);
        handler.addNewFile(old1.getFilename(), old1.getTier(), MANIFEST_PATH);
        handler.addNewFile(old2.getFilename(), old2.getTier(), MANIFEST_PATH);

        handler.postCompactationRebuild(List.of(old1, old2), newFile, MANIFEST_PATH);
        Map<String, Integer> tiers = handler.getFileTiers();
        assertFalse(tiers.containsKey(old1.getFilename()));
        assertFalse(tiers.containsKey(old2.getFilename()));
        assertEquals(2, tiers.get(newFile.getFilename()));
    }
}