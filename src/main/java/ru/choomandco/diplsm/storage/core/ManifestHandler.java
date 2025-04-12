package ru.choomandco.diplsm.storage.core;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ManifestHandler {

    private Map<String, Integer> fileLevels = new HashMap<>();

    /**
     * Method reads MANIFEST file. In case if file doesn't exist, method creates file and scans
     * existing folders and files and writes new MANIFEST file, if necessary
     */
    void readManifest(String manifestFilpath) {
        File file = new File(manifestFilpath);

        if (file.exists()) {
            loadExistingManifest(file);
        } else {
            System.out.println("No MANIFEST file, scanning dirs...");
            rebuildManifest(manifestFilpath);
        }
    }

    /**
     * Method loads data from MANIFEST file
     * @param manifestFile MANIFEST file
     */
    private void loadExistingManifest(File manifestFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(manifestFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                if (parts.length == 2) {
                    String fileName = parts[1];
                    int level = Integer.parseInt(parts[0].substring(1));
                    fileLevels.put(fileName, level);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while reading MANIFEST file - ", e);
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
    private void rebuildManifest(String manifestFile) {
        File baseDir = new File(manifestFile);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new RuntimeException("SSTable dir not found: " + manifestFile);
        }

        File[] levelDirs = baseDir.listFiles(File::isDirectory);
        if (levelDirs != null) {
            for (File levelDir : levelDirs) {
                if (levelDir.getName().matches("L\\d+")) {
                    int level = Integer.parseInt(levelDir.getName().substring(1));
                    File[] sstableFiles = levelDir.listFiles((dir, name) -> name.endsWith(".dat"));

                    if (sstableFiles != null) {
                        for (File sstableFile : sstableFiles) {
                            fileLevels.put(sstableFile.getName(), level);
                        }
                    }
                }
            }
        }

        writeManifestToFile(manifestFile);
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
    private void writeManifestToFile(String manifestFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(manifestFile))) {
            for (Map.Entry<String, Integer> entry : fileLevels.entrySet()) {
                writer.write("L" + entry.getValue() + " " + entry.getKey());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while writing MANIFEST file - ", e);
        }
    }

    void increaseLevelOfTable(String table, String manifestFile) {
        fileLevels.put(table, fileLevels.get(table) + 1);
        writeManifestToFile(manifestFile);
    }

    void increaseLevelOfTable(List<String> tables, String manifestFile) {
        for (String table : tables) {
            fileLevels.put(table, fileLevels.get(table) + 1);
        }
        writeManifestToFile(manifestFile);
    }
}
