/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author dcy
 * Create Date: 2021-08-25
 */
public class ThreadPoolStatistic {
    private static final ThreadPoolStatistic FRONT_BUSINESS = new ThreadPoolStatistic();
    private static final ThreadPoolStatistic FRONT_MANAGER = new ThreadPoolStatistic();
    private static final ThreadPoolStatistic BACKEND_BUSINESS = new ThreadPoolStatistic();
    private static final ThreadPoolStatistic WRITE_TO_BACKEND = new ThreadPoolStatistic();
    private LongAdder completedTaskCount = new LongAdder();

    public static ThreadPoolStatistic getFrontBusiness() {
        return FRONT_BUSINESS;
    }

    public static ThreadPoolStatistic getFrontManager() {
        return FRONT_MANAGER;
    }

    public static ThreadPoolStatistic getBackendBusiness() {
        return BACKEND_BUSINESS;
    }

    public static ThreadPoolStatistic getWriteToBackend() {
        return WRITE_TO_BACKEND;
    }

    public LongAdder getCompletedTaskCount() {
        return completedTaskCount;
    }


}
