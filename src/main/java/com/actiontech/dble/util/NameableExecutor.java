/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.util;

import com.actiontech.dble.singleton.ThreadChecker;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author mycat
 */
public class NameableExecutor extends ThreadPoolExecutor {

    protected String name;

    private Map<String, Map<Thread, Runnable>> runnableMap;
    private ThreadChecker threadChecker = null;

    public NameableExecutor(String name, int size, int maximumPoolSize, long keepAliveTime,
                            BlockingQueue<Runnable> queue, ThreadFactory factory, Map<String, Map<Thread, Runnable>> runnableMap, ThreadChecker threadChecker) {
        super(size, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, factory);
        this.name = name;
        this.runnableMap = runnableMap;
        this.threadChecker = threadChecker;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (threadChecker != null) {
            threadChecker.startExec(t);
        }
        if (null != runnableMap) {
            Map<Thread, Runnable> map = Maps.newConcurrentMap();
            map.put(t, r);
            Map<Thread, Runnable> oldVal = runnableMap.putIfAbsent(name, map);
            if (null != oldVal) {
                oldVal.put(t, r);
            }
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (threadChecker != null) {
            threadChecker.endExec();
        }
    }

    @Override
    protected void terminated() {
        super.terminated();
        if (threadChecker != null) {
            threadChecker.terminated();
        }
    }
}
