package ru.choomandco.diplsm.storage.sstable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {
    private static final String TEST_FILE = "./data/lsm/tables/T0/sstable_test_0.dat";
    private SSTable sstable;

    @BeforeEach
    void setUp() {
        sstable = new SSTable();
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(Path.of(TEST_FILE));
    }

    @Test
    void testWriteAndReadWholeMap() {
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");

        sstable.writeTableFromMap(data, TEST_FILE);
        Map<String, String> result = null;
        try {
            result = sstable.readWholeIntoMap(TEST_FILE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertEquals(2, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
    }

    @Test
    void testGetByKeyExisting() {
        Map<String, String> data = new HashMap<>();
        data.put("alpha", "beta");
        sstable.writeTableFromMap(data, TEST_FILE);

        String value = sstable.getByKey("alpha", TEST_FILE);
        assertEquals("beta", value);
    }

    @Test
    void testGetByKeyNonExisting() {
        Map<String, String> data = new HashMap<>();
        data.put("foo", "bar");
        sstable.writeTableFromMap(data, TEST_FILE);

        String value = sstable.getByKey("unknown", TEST_FILE);
        assertNull(value);
    }

    @Test
    void testReadStringsIntoList() {
        Map<String, String> data = new HashMap<>();
        data.put("one", "1");
        data.put("two", "2");
        sstable.writeTableFromMap(data, TEST_FILE);

        List<String> lines = null;
        try {
            lines = sstable.readStringsIntoList(TEST_FILE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertTrue(lines.stream().anyMatch(l -> l.equals("one=1")));
        assertTrue(lines.stream().anyMatch(l -> l.equals("two=2")));
    }

    @Test
    void testDeleteFile() throws IOException {
        Map<String, String> data = Map.of("k", "v");
        sstable.writeTableFromMap(data, TEST_FILE);
        assertTrue(Files.exists(Path.of(TEST_FILE)));

        sstable.deleteFIle(TEST_FILE);
        assertFalse(Files.exists(Path.of(TEST_FILE)));
    }

    @Test
    void testInvalidCRCThrows() throws IOException {
        Map<String, String> data = Map.of("a", "b");
        sstable.writeTableFromMap(data, TEST_FILE);
        Files.writeString(Path.of(TEST_FILE), "corrupt", java.nio.file.StandardOpenOption.APPEND);

        assertThrows(RuntimeException.class, () -> sstable.getByKey("a", TEST_FILE));
        assertThrows(RuntimeException.class, () -> sstable.readWholeIntoMap(TEST_FILE));
        assertThrows(RuntimeException.class, () -> sstable.readStringsIntoList(TEST_FILE));
    }
}
