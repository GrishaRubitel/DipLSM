package ru.choomandco.diplsm.storage.compengine;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.choomandco.diplsm.storage.sstable.SSTable;
import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CompactationEngineTest {
    private static final String TEST_DIR = ".\\data_test\\lsm\\tables";
    private CompactationEngine engine;

    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectories(Paths.get(TEST_DIR));
        engine = new CompactationEngine();
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(".\\data_test"));
    }

    @Test
    void testCompactEmptyListThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> engine.compact(Collections.emptyList(), TEST_DIR + "\\out.dat", 1));
    }

    @Test
    void testCompactMergesAndDeletes() {
        SSTableMetadata meta1 = writeSampleTable("s1.dat", Map.of("a", "1", "b", "2"));
        SSTableMetadata meta2 = writeSampleTable("s2.dat", Map.of("b", "3", "c", "4"));

        List<SSTableMetadata> metas = List.of(meta1, meta2);
        String output = TEST_DIR + "\\merged.dat";

        SSTableMetadata resultMeta = engine.compact(metas, output, 2);

        assertEquals(output, resultMeta.getFilename());
        assertEquals(2, resultMeta.getTier());

        SSTable sstable = new SSTable();
        Map<String, String> merged = sstable.readWholeIntoMap(output);
        assertEquals(3, merged.size());
        assertEquals("1", merged.get("a"));
        assertEquals("3", merged.get("b"));
        assertEquals("4", merged.get("c"));

        assertFalse(Files.exists(Path.of(meta1.getFilename())));
        assertFalse(Files.exists(Path.of(meta2.getFilename())));
    }

    private SSTableMetadata writeSampleTable(String filename, Map<String, String> data) {
        String path = TEST_DIR + "\\" + filename;
        new SSTable().writeTableFromMap(data, path);
        return new SSTableMetadata(path, 1, data.keySet());
    }
}
