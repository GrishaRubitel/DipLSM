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

public class MixedLoadTest {
    private final DipLSMStorage storage;
    private final long durationMs;
    private final int checkEvery;

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong mismatches = new AtomicLong();
    private final AtomicLong totalPutNs = new AtomicLong();
    private final AtomicLong totalGetNs = new AtomicLong();

    public MixedLoadTest(DipLSMStorage storage, long durationMs, int checkEvery) {
        this.storage    = storage;
        this.durationMs = durationMs;
        this.checkEvery = checkEvery;
    }

    public void run() {
        System.out.printf("=== MIXED LOAD TEST: duration=%d ms ===%n", durationMs);
        Random rnd = new Random();
        ConcurrentMap<String,String> reference = new ConcurrentHashMap<>();

        long end = System.currentTimeMillis() + durationMs;
        long ops = 0;

        while (System.currentTimeMillis() < end) {
            String key = "key-" + rnd.nextInt(10_000);

            // PUT
            String value = UUID.randomUUID().toString();
            long t0 = System.nanoTime();
            storage.put(key, value);
            totalPutNs.addAndGet(System.nanoTime() - t0);
            puts.incrementAndGet();
            reference.put(key, value);

            // GET
            t0 = System.nanoTime();
            String got = storage.get(key);
            totalGetNs.addAndGet(System.nanoTime() - t0);
            gets.incrementAndGet();

            if (++ops % checkEvery == 0) {
                String exp = reference.get(key);
                String act = storage.get(key);
                if (exp != null && !Objects.equals(exp, act)) {
                    System.err.printf("Mismatch key=%s expected=%s actual=%s%n", key, exp, act);
                    mismatches.incrementAndGet();
                }
            }
        }

        // Итоги
        double avgPutMs = totalPutNs.get()/1_000_000.0/puts.get();
        double avgGetMs = totalGetNs.get()/1_000_000.0/gets.get();

        System.out.println("=== MIXED TEST COMPLETED ===");
        System.out.printf("Duration: %d ms%n", durationMs);
        System.out.printf("Operations: %d (puts=%d, gets=%d)%n",
                puts.get()+gets.get(), puts.get(), gets.get());
        System.out.printf("Avg put latency = %.6f ms%n", avgPutMs);
        System.out.printf("Avg get latency = %.6f ms%n", avgGetMs);
        System.out.printf("Mismatches: %d%n%n", mismatches.get());
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

    public static void main(String[] args) throws InterruptedException {
        int  checkEvery = 1_000;

        System.out.println("Deleting ./data");
        deleteDataDirectory(new File("./data"));
        Thread.sleep(100);

        int duration = 60_000;
        new MixedLoadTest(new StorageCoreAsync(1L*1024*1024, 5), duration, checkEvery).run();

        System.out.println("Deleting ./data");
        deleteDataDirectory(new File("./data"));
        Thread.sleep(100);

        duration = 600_000;
        new MixedLoadTest(new StorageCoreAsync(1L*1024*1024, 5), duration, checkEvery).run();
    }
}
