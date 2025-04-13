package ru.choomandco.diplsm.storage.bloomfilter;

import java.util.BitSet;
import java.util.Set;
import java.util.function.Function;

public class BloomFilter<T> {
    private final BitSet bitSet;
    private final Function<T, Integer>[] hashFunctions;

    public BloomFilter(int size, Function<T, Integer>[] hashFunctions) {
        this.bitSet = new BitSet(size);
        this.hashFunctions = hashFunctions;
    }

    public void add(T key) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int hash = hashFunction.apply(key);
            bitSet.set(Math.abs(hash) % bitSet.size(), true);
        }
    }

    public boolean mightContain(T key) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int hash = hashFunction.apply(key);
            if (!bitSet.get(Math.abs(hash) % bitSet.size())) {
                return false;
            }
        }
        return true;
    }

    public void addKeysFromMap(Set<T> keySet) {
        for (T key : keySet) {
            for (Function<T, Integer> hashFunction : hashFunctions) {
                int hash = hashFunction.apply(key);
                bitSet.set(Math.abs(hash) % bitSet.size(), true);
            }
        }
    }
}
