/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.Map;
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

    public static NameableExecutor createFixed(String name, int size, Map<String, Map<Thread, Runnable>> runnableMap) {
        return createFixed(name, size, true, runnableMap);
    }

    private static NameableExecutor createFixed(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, size, Long.MAX_VALUE, new LinkedBlockingQueue<Runnable>(), factory);
    }

    private static NameableExecutor createFixed(String name, int size, boolean isDaemon, Map<String, Map<Thread, Runnable>> runnableMap) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, size, Long.MAX_VALUE, new LinkedBlockingQueue<Runnable>(), factory, runnableMap);
    }

    public static NameableExecutor createCached(String name, int size) {
        return createCached(name, size, true);
    }

    public static NameableExecutor createCached(String name, int size, Map<String, Map<Thread, Runnable>> runnableMap) {
        return createCached(name, size, true, runnableMap);
    }

    private static NameableExecutor createCached(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, Integer.MAX_VALUE, 60, new SynchronousQueue<Runnable>(), factory);
    }

    private static NameableExecutor createCached(String name, int size, boolean isDaemon, Map<String, Map<Thread, Runnable>> runnableMap) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, Integer.MAX_VALUE, 60, new SynchronousQueue<Runnable>(), factory, runnableMap);
    }
}
