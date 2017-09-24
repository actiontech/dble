/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author mycat
 */
public class NameableExecutor extends ThreadPoolExecutor {

    protected String name;

    public NameableExecutor(String name, int size, int maximumPoolSize, long keepAliveTime,
                            BlockingQueue<Runnable> queue, ThreadFactory factory) {
        super(size, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, factory);
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
