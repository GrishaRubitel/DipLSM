package ru.choomandco.diplsm;

import ru.choomandco.diplsm.storage.core.StorageCore;
import ru.choomandco.diplsm.storage.core.StorageCoreAsync;
import ru.choomandco.diplsm.storage.interfaces.DipLSMStorage;

import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTest {

    private final DipLSMStorage storage;
    // Эталонная карта в памяти
    private final ConcurrentMap<String, String> reference = new ConcurrentHashMap<>();

    private final Random rnd = new Random();
    private final long testDurationMillis;
    private final int checkEvery; // каждые N операций — проверка

    // Счётчики операций
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong mismatches = new AtomicLong();

    // Временные метрики (наносекунды)
    private final AtomicLong totalPutTime = new AtomicLong();
    private final AtomicLong totalGetTime = new AtomicLong();

    public LoadTest(DipLSMStorage storage, long testDurationMillis, int checkEvery) {
        this.storage = storage;
        this.testDurationMillis = testDurationMillis;
        this.checkEvery = checkEvery;
    }

    public void run() {
        long endTime = System.currentTimeMillis() + testDurationMillis;
        long ops = 0;

        while (System.currentTimeMillis() < endTime) {
            String key = "key-" + rnd.nextInt(10_000);
            // 50/50 PUT или GET
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

            // Периодическая контрольная проверка
            if (++ops % checkEvery == 0) {
                String ref = reference.get(key);
                String actual = storage.get(key);
                System.out.printf("Control check key=%s: expected=%s, actual=%s%n",
                        key, ref, actual);
                if (ref != null & actual != null & !Objects.equals(ref, actual)) {
                    System.err.println("Mismatch for key");
                    mismatches.incrementAndGet();
                }
            }
        }

        // Вычисляем средние времена
        long putCount = puts.get();
        long getCount = gets.get();

        double avgPutMs = putCount > 0 ? (totalPutTime.get() / 1_000_000.0) / putCount : 0.0;
        double avgGetMs = getCount > 0 ? (totalGetTime.get() / 1_000_000.0) / getCount : 0.0;

        // Итоги
        System.out.println("=== Load Test Completed ===");
        System.out.printf("Duration (ms): %d%n", testDurationMillis);
        System.out.printf("Operations:    %d (total)%n", ops);
        System.out.printf("Puts:          %d, avg time = %.8f ms%n", putCount, avgPutMs);
        System.out.printf("Gets:          %d, avg time = %.8f ms%n", getCount, avgGetMs);
        System.out.printf("Mismatches:    %d%n", mismatches.get());
    }

    public static void main(String[] args) {
        long duration = 1L * 60_000L;
        int checkEvery = 1_000;

        if (args.length >= 1) {
            duration = Long.parseLong(args[0]);
        }
        if (args.length >= 2) {
            checkEvery = Integer.parseInt(args[1]);
        }

        DipLSMStorage storage = new StorageCoreAsync(500L, 3);

        LoadTest test = new LoadTest(storage, duration, checkEvery);
        test.run();
    }
}
