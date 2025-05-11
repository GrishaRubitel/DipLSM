package ru.choomandco.diplsm.storage.bloomfilter;

import java.util.BitSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Простая реализация фильтра Блума — вероятностной структуры данных,
 * предназначенной для проверки принадлежности элемента множеству.
 *
 * @param <T> тип элементов, добавляемых в фильтр
 */
public class BloomFilter<T> {
    /** Внутреннее битовое представление фильтра */
    private final BitSet bitSet;
    /** Массив хеш-функций, используемых для установки битов */
    private final Function<T, Integer>[] hashFunctions;

    /**
     * Создаёт новый фильтр Блума заданного размера и с переданным набором хеш-функций.
     *
     * @param size размер битового массива
     * @param hashFunctions массив хеш-функций, применяемых к ключам
     */
    public BloomFilter(int size, Function<T, Integer>[] hashFunctions) {
        this.bitSet = new BitSet(size);
        this.hashFunctions = hashFunctions;
    }

    /**
     * Добавляет ключ в фильтр. Для каждого ключа используются все переданные хеш-функции.
     *
     * @param key ключ, который нужно добавить
     */
    public void add(T key) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int hash = hashFunction.apply(key);
            bitSet.set(Math.abs(hash) % bitSet.size(), true);
        }
    }

    /**
     * Проверяет, может ли фильтр содержать данный ключ.
     * Возвращает {@code true}, если ключ, возможно, присутствует;
     * {@code false}, если точно отсутствует.
     *
     * @param key ключ для проверки
     * @return {@code true} если ключ может присутствовать, {@code false} — если точно отсутствует
     */
    public boolean mightContain(T key) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int hash = hashFunction.apply(key);
            if (!bitSet.get(Math.abs(hash) % bitSet.size())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Добавляет все ключи из заданного множества в фильтр.
     * Использует все хеш-функции для каждого ключа.
     *
     * @param keySet множество ключей, которые необходимо добавить
     */
    public void addKeysFromMap(Set<T> keySet) {
        for (T key : keySet) {
            for (Function<T, Integer> hashFunction : hashFunctions) {
                int hash = hashFunction.apply(key);
                bitSet.set(Math.abs(hash) % bitSet.size(), true);
            }
        }
    }
}
