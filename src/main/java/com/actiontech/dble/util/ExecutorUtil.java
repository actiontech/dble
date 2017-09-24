/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * @author mycat
 */
public final class ExecutorUtil {
    private ExecutorUtil() {
    }

    public static NameableExecutor createFixed(String name, int size) {
        return createFixed(name, size, true);
    }

    private static NameableExecutor createFixed(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, size, Long.MAX_VALUE, new LinkedBlockingQueue<Runnable>(), factory);
    }

    public static NameableExecutor createCached(String name, int size) {
        return createCached(name, size, true);
    }

    private static NameableExecutor createCached(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, Integer.MAX_VALUE, 60, new SynchronousQueue<Runnable>(), factory);
    }
}
