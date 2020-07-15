/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by szf on 2018/3/6.
 * this class is save the DistributeLock when ddl executed
 */
public final class DistributeLockManager {

    private final Map<String, DistributeLock> lockTables;
    private static final DistributeLockManager INSTANCE = new DistributeLockManager();

    private DistributeLockManager() {
        this.lockTables = new HashMap<>();
    }

    public static void addLock(DistributeLock lock) {
        INSTANCE.lockTables.put(lock.getPath(), lock);
    }

    public static void releaseLock(String path) {
        DistributeLock removedLock = INSTANCE.lockTables.remove(path);
        if (removedLock != null) {
            removedLock.release();
        }
    }

    public static boolean isLooked(String path) {
        return INSTANCE.lockTables.containsKey(path);
    }
}
