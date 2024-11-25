/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.util;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.*;

/**
 * @author mycat
 */
public class NameableExecutor extends ThreadPoolExecutor {

    protected String name;

    private Map<String, Map<Thread, Runnable>> runnableMap;

    public NameableExecutor(String name, int size, int maximumPoolSize, long keepAliveTime,
                            BlockingQueue<Runnable> queue, ThreadFactory factory, Map<String, Map<Thread, Runnable>> runnableMap) {
        super(size, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, factory);
        this.name = name;
        this.runnableMap = runnableMap;
    }

    public String getName() {
        return name;
    }


    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        if (null != runnableMap) {
            Map<Thread, Runnable> map = Maps.newConcurrentMap();
            map.put(t, r);
            Map<Thread, Runnable> oldVal = runnableMap.putIfAbsent(name, map);
            if (null != oldVal) {
                oldVal.put(t, r);
            }
        }
    }
}
