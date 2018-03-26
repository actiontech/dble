package com.actiontech.dble.config.loader.ucoreprocess;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by szf on 2018/3/6.
 * this class is save the UDistributeLock when ddl executed
 */
public final class UDistrbtLockManager {

    private final Map<String, UDistributeLock> lockTables;
    private static final UDistrbtLockManager INSTANCE = new UDistrbtLockManager();

    private UDistrbtLockManager() {
        this.lockTables = new HashMap<String, UDistributeLock>();
    }

    public static void addLock(UDistributeLock lock) {
        INSTANCE.lockTables.put(lock.getPath(), lock);
    }

    public static void releaseLock(String path) {
        INSTANCE.lockTables.get(path).release();
    }
}
