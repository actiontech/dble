/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;

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
