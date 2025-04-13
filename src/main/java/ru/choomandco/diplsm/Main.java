package ru.choomandco.diplsm;

import ru.choomandco.diplsm.storage.core.StorageCore;

public class Main {
    public static void main(String[] args) {
        StorageCore lsmCore = new StorageCore(20L);

        for (int i = 1; i <= 20; i++) {
            String key = "user_" + i;
            String value = "User Name " + i;
            lsmCore.put(key, value);
        }

        for (int i = 1; i <= 20; i++) {
            String key = "user_" + i;
            System.out.println("Key  - " + key + "; Value - " + lsmCore.get(key));
        }
    }
}