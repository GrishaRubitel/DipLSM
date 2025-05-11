package ru.choomandco.diplsm;

import ru.choomandco.diplsm.storage.core.StorageCoreAsync;
import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;

import java.io.File;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTest {

    private final DipLSMStorage storage;
    private final ConcurrentMap<String, String> reference = new ConcurrentHashMap<>();
    private final Random rnd = new Random();
    private final long testDurationMillis;
    private final int checkEvery;

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong mismatches = new AtomicLong();
    private final AtomicLong totalPutTime = new AtomicLong();
    private final AtomicLong totalGetTime = new AtomicLong();

    public LoadTest(DipLSMStorage storage, long testDurationMillis, int checkEvery) {
        this.storage = storage;
        this.testDurationMillis = testDurationMillis;
        this.checkEvery = checkEvery;
    }

    public void run() {
        long endTime = System.currentTimeMillis() + testDurationMillis;
        long nextStatusTime = System.currentTimeMillis() + 20_000;
        long ops = 0;

        while (System.currentTimeMillis() < endTime) {
            String key = "key-" + rnd.nextInt(10_000);

            if (rnd.nextBoolean()) {
                // PUT
                String value = UUID.randomUUID().toString();
                long start = System.nanoTime();
                storage.put(key, value);
                long duration = System.nanoTime() - start;
                totalPutTime.addAndGet(duration);
                reference.put(key, value);
                puts.incrementAndGet();
            } else {
                // GET
                long start = System.nanoTime();
                String fromStore = storage.get(key);
                long duration = System.nanoTime() - start;
                totalGetTime.addAndGet(duration);
                gets.incrementAndGet();
            }

            // Периодическая проверка
            if (++ops % checkEvery == 0) {
                String ref = reference.get(key);
                String actual = storage.get(key);
                if (ref != null && actual != null && !Objects.equals(ref, actual)) {
                    System.err.printf("Mismatch for key=%s: expected=%s, actual=%s%n", key, ref, actual);
                    mismatches.incrementAndGet();
                }
            }

            // Признак жизни
            if (System.currentTimeMillis() >= nextStatusTime) {
                System.out.println("Test in progress...");
                nextStatusTime += 20_000;
            }
        }

        long putCount = puts.get();
        long getCount = gets.get();

        double avgPutMs = putCount > 0 ? (totalPutTime.get() / 1_000_000.0) / putCount : 0.0;
        double avgGetMs = getCount > 0 ? (totalGetTime.get() / 1_000_000.0) / getCount : 0.0;

        System.out.println("=== Load Test Completed ===");
        System.out.printf("Duration (ms): %d%n", testDurationMillis);
        System.out.printf("Operations:    %d (total)%n", ops);
        System.out.printf("Puts:          %d, avg time = %.8f ms%n", putCount, avgPutMs);
        System.out.printf("Gets:          %d, avg time = %.8f ms%n", getCount, avgGetMs);
        System.out.printf("Mismatches:    %d%n", mismatches.get());
    }

    private static void deleteDataDirectory(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDataDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    public static void main(String[] args) {
//        // Удаление ./data
//        System.out.println("Deleting ./data directory...");
//        deleteDataDirectory(new File("./data"));

        // Первая фаза — 1 минута
        System.out.println("=== Phase 1: short run ===");
        DipLSMStorage storage1 = new StorageCoreAsync(1L * 256 * 1024, 5);
        new LoadTest(storage1, 60_000L, 1_000).run();

//        // Удаление ./data
//        System.out.println("Deleting ./data directory...");
//        deleteDataDirectory(new File("./data"));

        // Вторая фаза — 10 минут
        System.out.println("=== Phase 2: long run ===");
        DipLSMStorage storage2 = new StorageCoreAsync(5L * 1024 * 1024, 5);
        new LoadTest(storage2, 600_000L, 1_000).run();
    }
}
