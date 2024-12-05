/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;


import javax.annotation.Nonnull;

/**
 * Created by szf on 2020/6/18.
 */
public abstract class ServiceTask implements Comparable<ServiceTask> {

    /**
     * used for sort
     */
    private long taskId = -1;
    protected final Service service;


    protected ServiceTask(Service service) {
        this.service = service;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskIdTmp) {
        this.taskId = taskIdTmp;
    }

    public Service getService() {
        return service;
    }

    @Nonnull
    public abstract ServiceTaskType getType();

    @Override
    public int compareTo(ServiceTask o) {
        return Long.compare(taskId, o.taskId);
    }

    @Override
    public String toString() {
        return "ServiceTask{" +
                "taskId=" + taskId +
                ", type=" + getType() +
                ", service=" + service +
                '}';
    }
}
