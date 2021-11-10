/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

import org.jetbrains.annotations.NotNull;

/**
 * @author dcy
 * Create Date: 2021-08-27
 */
public class DelayedServiceTask extends InnerServiceTask {
    private ServiceTask originTask;

    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.DELAYED;
    }

    public DelayedServiceTask(ServiceTask task) {
        super(task.getService());
        this.originTask = task;
        this.setTaskId(originTask.getTaskId());
    }


    public ServiceTask getOriginTask() {
        return originTask;
    }
}
