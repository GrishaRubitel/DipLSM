package ru.choomandco.diplsm.storage.core;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

//TODO дописать сохранение метаданных в манифест, проверить сбор метаданных при пустом манифесте
//TODO проверить работоспособность манифеста вообще
class ManifestHandler {
    private static final String MANIFEST_PATH = "./data/lsm/MANIFEST";

    private final Map<String, Integer> fileTiers = new HashMap<>();

    public Map<String, Integer> getFileTiers() {
        return fileTiers;
    }

    /**
     * Method reads MANIFEST file. In case if file doesn't exist, method creates file and scans
     * existing folders and files and writes new MANIFEST file, if necessary
     */
    public void readManifest(String tablesPath) {
        File file = new File(MANIFEST_PATH);

        if (file.exists()) {
            if (!loadAndVerifyManifest()) {
                System.out.println("MANIFEST corrupted or invalid CRC, rebuilding...");
                rebuildManifest(tablesPath);
            }
        } else {
            System.out.println("No MANIFEST found, scanning...");
            rebuildManifest(tablesPath);
        }
    }

    /**
     * Method loads data from MANIFEST file
     */
    private boolean loadAndVerifyManifest() {
        try (BufferedReader reader = new BufferedReader(new FileReader(MANIFEST_PATH))) {
            String crcLine = reader.readLine();
            if (crcLine == null || !crcLine.startsWith("#CRC=")) return false;

            long expectedCRC = Long.parseLong(crcLine.substring(5));
            CRC32 crc = new CRC32();

            String line;
            List<String> entries = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                crc.update(line.getBytes());
                entries.add(line);
            }

            if (crc.getValue() != expectedCRC) return false;

            for (String entry : entries) {
                String[] parts = entry.split(" ");
                if (parts.length == 2) {
                    String fileName = parts[1];
                    int tier = Integer.parseInt(parts[0].substring(1));
                    fileTiers.put(fileName, tier);
                }
            }
            return true;
        } catch (IOException e) {
            System.err.println("Error reading MANIFEST: " + e.getMessage());
            return false;
        }
    }

    /**
     * Rebuilds the SSTable manifest by scanning the SSTable directory structure.
     * It searches for directories representing different levels (e.g., L1, L2, etc.),
     * and adds all SSTable files found in these directories to the manifest.
     * The method then writes the updated manifest to the SSTABLE_MANIFEST_FILE.
     *
     * This method is called when the manifest file is missing or needs to be rebuilt
     * from the existing SSTable files.
     */
    private void rebuildManifest(String tablesPath) {
        File baseDir = new File(tablesPath);
        File parent = baseDir.getParentFile();

        if (!parent.exists()) {
            try {
                Files.createDirectories(parent.toPath());
            } catch (IOException e) {
                throw new RuntimeException("Cannot create directory for MANIFEST", e);
            }
        }

        File[] tierDirs = parent.listFiles(File::isDirectory);
        if (tierDirs != null) {
            for (File dir : tierDirs) {
                if (dir.getName().matches("T\\d+")) {
                    int tier = Integer.parseInt(dir.getName().substring(1));
                    File[] sstables = dir.listFiles((d, name) -> name.endsWith(".dat"));
                    if (sstables != null) {
                        for (File sstable : sstables) {
                            fileTiers.put(sstable.getName(), tier);
                        }
                    }
                }
            }
        }

        writeManifest();
    }

    /**
     * Writes the current state of the file levels to the SSTABLE_MANIFEST_FILE.
     * This method iterates over the fileLevels map and records each SSTable file's
     * name along with its corresponding level in the manifest file.
     * The manifest file is completely overwritten with the updated contents.
     *
     * This method is typically called when the file structure needs to be recorded or
     * when the manifest needs to be generated from scratch.
     *
     * @throws RuntimeException if an I/O error occurs while writing the manifest file
     */
    private void writeManifest() {
        List<String> entries = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fileTiers.entrySet()) {
            entries.add("T" + entry.getValue() + " " + entry.getKey());
        }

        CRC32 crc = new CRC32();
        for (String entry : entries) {
            crc.update(entry.getBytes());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(MANIFEST_PATH))) {
            writer.write("#CRC=" + crc.getValue());
            writer.newLine();
            for (String entry : entries) {
                writer.write(entry);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing MANIFEST", e);
        }
    }

    public void updateFileTier(String filename, int newTier) {
        fileTiers.put(filename, newTier);
        writeManifest();
    }

    public void updateFilesTier(List<String> filenames, int newTier) {
        for (String filename : filenames) {
            fileTiers.put(filename, newTier);
        }
        writeManifest();
    }

    public void addNewFile(String filename, int tier) {
        fileTiers.put(filename, tier);
        writeManifest();
    }
}
