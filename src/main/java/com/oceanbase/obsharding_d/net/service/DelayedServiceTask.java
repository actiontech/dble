/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

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
