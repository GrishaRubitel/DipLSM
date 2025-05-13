package ru.choomandco.diplsm;

import ru.choomandco.diplsm.storage.core.StorageCoreAsync;
import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PutOnlyLoadTest {
    private final DipLSMStorage storage;
    private final long durationMs;
    private final long checkEvery;

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong totalPutNs = new AtomicLong();

    public PutOnlyLoadTest(DipLSMStorage storage, long durationMs, long checkEvery) {
        this.storage    = storage;
        this.durationMs = durationMs;
        this.checkEvery = checkEvery;
    }

    public void run() {
        List<Long> memMetrx = new ArrayList<>();
        System.out.printf("=== PUT-ONLY TEST: duration=%d ms, sample every %d ops ===%n",
                durationMs, checkEvery);

        Random rnd = new Random();
        long end = System.currentTimeMillis() + durationMs;
        long ops = 0;

        System.out.printf("Memory before run: %d MB%n", usedMemoryMB());

        while (System.currentTimeMillis() < end) {
            String key = "key-" + rnd.nextInt(10_000);
            String value = UUID.randomUUID().toString();

            long t0 = System.nanoTime();
            storage.put(key, value);
            totalPutNs.addAndGet(System.nanoTime() - t0);
            puts.incrementAndGet();

            if (++ops % checkEvery == 0) {
                long l = usedMemoryMB();
                System.out.println(l);
                memMetrx.add(l);
            }
        }

        double avgPutMs = totalPutNs.get()/1_000_000.0/puts.get();

        System.out.println("=== PUT-ONLY TEST COMPLETED ===");
        System.out.printf("Duration: %d ms%n", durationMs);
        System.out.printf("Total puts: %d, avg latency = %.6f ms%n",
                puts.get(), avgPutMs);
        System.out.printf("Memory after run: %d MB%n%n", usedMemoryMB());
        System.out.println("Mem metrics:");
        for (Long l : memMetrx) {
            System.out.println(l);
        }
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

    private long usedMemoryMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    }

    public static void main(String[] args) throws InterruptedException {
        long checkEvery = 1_000_000L;

        System.out.println("Deleting ./data");
        deleteDataDirectory(new File("./data"));
        Thread.sleep(100);

        System.gc();
        int duration = 60_000;
        new PutOnlyLoadTest(new StorageCoreAsync(6L*1024*1024, 3), duration, checkEvery).run();

        System.gc();
        System.out.println("Deleting ./data");
        deleteDataDirectory(new File("./data"));
        Thread.sleep(100);

        duration = 600_000;
        new PutOnlyLoadTest(new StorageCoreAsync(6L*1024*1024, 3), duration, checkEvery).run();
    }
}
