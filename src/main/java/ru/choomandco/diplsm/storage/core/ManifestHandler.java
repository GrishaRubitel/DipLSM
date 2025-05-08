package ru.choomandco.diplsm.storage.core;

import ru.choomandco.diplsm.storage.sstable.SSTableMetadata;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

class ManifestHandler {
    private final Map<String, Integer> fileTiers = new HashMap<>();

    public Map<String, Integer> getFileTiers() {
        return fileTiers;
    }

    /**
     * Метод читает файл MANIFEST. В случае, если файл не существует, метод создает файл и
     * сканирует существующие папки и файлы и записывает новый файл MANIFEST, если необходимо
     */
    public void readManifest(String tablesPath, String manifestPath) {
        File file = new File(manifestPath);

        if (file.exists()) {
            if (!loadAndVerifyManifest(manifestPath)) {
                System.out.println("MANIFEST corrupted or invalid CRC, rebuilding...");
                rebuildManifest(tablesPath, manifestPath);
            }
        } else {
            System.out.println("No MANIFEST found, scanning...");
            rebuildManifest(tablesPath, manifestPath);
        }
    }

    /**
     * Метод загружает информацию из MANIFEST
     */
    private boolean loadAndVerifyManifest(String manifestPath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(manifestPath))) {
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
     * Переписывает MANIFEST, сканируя каталоги SSTable.
     * Он ищет каталоги, представляющие различные уровни (например, T1, T2 и T3),
     * и добавляет все файлы SSTable, найденные в этих каталогах, в манифест.
     * Затем метод записывает обновленный манифест в SSTABLE_MANIFEST_FILE.
     *
     * Этот метод вызывается, когда файл манифеста отсутствует или его необходимо перестроить
     * из существующих файлов SSTable.
     */
    private void rebuildManifest(String tablesPath, String manifestPath) {
        File baseDir = new File(tablesPath);
//        File parent = baseDir.getParentFile();
//
//        if (!parent.exists()) {
//            try {
//                Files.createDirectories(parent.toPath());
//            } catch (IOException e) {
//                throw new RuntimeException("Cannot create directory for MANIFEST", e);
//            }
//        }

        File[] tierDirs = baseDir.listFiles(File::isDirectory);
        if (tierDirs != null) {
            for (File dir : tierDirs) {
                if (dir.getName().matches("T\\d+")) {
                    int tier = Integer.parseInt(dir.getName().substring(1));
                    File[] sstables = dir.listFiles((d, name) -> name.endsWith(".dat"));
                    if (sstables != null) {
                        for (File sstable : sstables) {
                            fileTiers.put(dir + "\\" + sstable.getName(), tier);
                        }
                    }
                }
            }
        }

        writeManifest(manifestPath);
    }

    /**
     * Записывает текущее состояние уровней файла в SSTABLE_MANIFEST_FILE.
     * Метод перебирает карту fileLevels и записывает имя каждого файла SSTable
     * вместе с его соответствующим уровнем в файле манифеста.
     * Файл манифеста полностью перезаписывается обновленным содержимым.
     *
     * Этот метод обычно вызывается, когда необходимо записать структуру файла или
     * когда необходимо сгенерировать манифест с нуля.
     *
     * @throws RuntimeException если при записи файла манифеста произошла ошибка ввода-вывода
     */
    private void writeManifest(String manifestPath) {
        List<String> entries = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fileTiers.entrySet()) {
            entries.add("T" + entry.getValue() + " " + entry.getKey());
        }

        CRC32 crc = new CRC32();
        for (String entry : entries) {
            crc.update(entry.getBytes());
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(manifestPath))) {
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

    public void updateFileTier(String filename, int newTier, String manifestPath) {
        fileTiers.put(filename, newTier);
        writeManifest(manifestPath);
    }

    public void addNewFile(String filename, int tier, String manifestPath) {
        fileTiers.put(filename, tier);
        writeManifest(manifestPath);
    }

    public void postCompactationRebuild(List<SSTableMetadata> listToDelete, SSTableMetadata newFile, String manifestPath) {
        fileTiers.put(newFile.getFilename(), newFile.getTier());
        for (SSTableMetadata meta : listToDelete) {
            fileTiers.remove(meta.getFilename());
        }
        writeManifest(manifestPath);
    }
}
