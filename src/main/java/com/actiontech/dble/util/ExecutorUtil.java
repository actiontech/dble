/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.util;

import com.actiontech.dble.singleton.ThreadChecker;
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
        return createFixed(name, null, size, true, null);
    }

    public static NameableExecutor createFixed(String name, int size, ThreadChecker checker) {
        return createFixed(name, null, size, true, null, checker);
    }

    public static NameableExecutor createFixed(String name, int size, Map<String, Map<Thread, Runnable>> runnableMap) {
        return createFixed(name, null, size, true, runnableMap);
    }

    public static NameableExecutor createFixed(String namePrefix, String nameSuffix, int size, Map<String, Map<Thread, Runnable>> runnableMap) {
        return createFixed(namePrefix, nameSuffix, size, true, runnableMap);
    }

    private static NameableExecutor createFixed(String namePrefix, String nameSuffix, int size, boolean isDaemon, Map<String, Map<Thread, Runnable>> runnableMap) {
        NameableThreadFactory factory = new NameableThreadFactory(namePrefix, nameSuffix, isDaemon);
        return new NameableExecutor(namePrefix, size, size, Long.MAX_VALUE, new LinkedBlockingQueue<>(), factory, runnableMap, null);
    }

    private static NameableExecutor createFixed(String namePrefix, String nameSuffix, int size, boolean isDaemon, Map<String, Map<Thread, Runnable>> runnableMap, ThreadChecker checker) {
        NameableThreadFactory factory = new NameableThreadFactory(namePrefix, nameSuffix, isDaemon);
        return new NameableExecutor(namePrefix, size, size, Long.MAX_VALUE, new LinkedBlockingQueue<>(), factory, runnableMap, checker);
    }

    public static NameableExecutor createCached(String name, int size) {
        return createCached(name, size, true, null);
    }

    public static NameableExecutor createCached(String name, int size, Map<String, Map<Thread, Runnable>> runnableMap) {
        return createCached(name, size, true, runnableMap);
    }

    private static NameableExecutor createCached(String name, int size, boolean isDaemon, Map<String, Map<Thread, Runnable>> runnableMap) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, Integer.MAX_VALUE, 60, new SynchronousQueue<>(), factory, runnableMap, null);
    }

    /**
     * When the size number of threads takes a long time to execute or hangs, and the queue is full,
     * create a new thread to continue working, and the number of newly created threads does not exceed maxSize
     * <p>
     * If maxSize threads are all hanging, only 1024 tasks will be stored in the queue (this avoids memory leaks),
     * and subsequent tasks will be discarded by default
     */
    public static NameableExecutor createCached(String name, int size, int maxSize, ThreadChecker checker) {
        NameableThreadFactory factory = new NameableThreadFactory(name, true);
        return new NameableExecutor(name, size, maxSize, 60, new LinkedBlockingQueue<>(256), factory, null, checker);
    }
}
