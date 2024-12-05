/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.executor;

/**
 * @author dcy
 * Create Date: 2021-08-25
 */
public class ThreadContext implements ThreadContextView {
    private volatile boolean doingTask = false;


    @Override
    public boolean isDoingTask() {
        return doingTask;
    }

    public void setDoingTask(boolean doingTaskTmp) {
        doingTask = doingTaskTmp;
    }
}
