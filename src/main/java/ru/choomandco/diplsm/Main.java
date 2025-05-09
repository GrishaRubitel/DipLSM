package ru.choomandco.diplsm;

import ru.choomandco.diplsm.storage.core.StorageCore;

public class Main {
    public static void main(String[] args) {
        StorageCore lsmCore = new StorageCore(100L, 5);

        for (int i = 1; i <= 1000; i++) {
            String key = "user_" + i;
            String value = "User Name 30" + i;
            lsmCore.put(key, value);
        }

        lsmCore.flush(0);

        for (int i = 1; i <= 20; i++) {
            String key = "user_" + i;
            System.out.println("Key  - " + key + "; Value - " + lsmCore.get(key));
        }

        System.out.println("=========================================");

//        lsmCore.flush(0);

//        for (int i = 1; i <= 10; i++) {
//            String key = "user_" + i;
//            String value = "User Name 40" + i;
//            lsmCore.put(key, value);
//        }
//
//        for (int i = 1; i <= 20; i++) {
//            String key = "user_" + i;
//            System.out.println("Key  - " + key + "; Value - " + lsmCore.get(key));
//        }

//        lsmCore.flush(0);
    }
}