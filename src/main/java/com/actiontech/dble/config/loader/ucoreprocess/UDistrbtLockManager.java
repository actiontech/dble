/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
        this.lockTables = new HashMap<>();
    }

    public static void addLock(UDistributeLock lock) {
        INSTANCE.lockTables.put(lock.getPath(), lock);
    }

    public static void releaseLock(String path) {
        UDistributeLock removedLock = INSTANCE.lockTables.remove(path);
        if (removedLock != null) {
            removedLock.release();
        }
    }
}
