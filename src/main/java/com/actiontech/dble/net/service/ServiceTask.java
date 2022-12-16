package com.actiontech.dble.net.service;

import javax.annotation.Nullable;

/**
 * Created by szf on 2020/6/18.
 */
public class ServiceTask {

    private final byte[] orgData;
    private volatile boolean highPriority = false;
    private final boolean reuse;
    private final Service service;
    private Integer sequenceId = null;

    public ServiceTask(byte[] orgData, Service service, @Nullable Integer sequenceId) {
        this.orgData = orgData;
        this.service = service;
        this.reuse = false;
        this.sequenceId = sequenceId;
    }

    public ServiceTask(byte[] orgData, Service service, boolean reuse) {
        this.orgData = orgData;
        this.service = service;
        this.reuse = reuse;
    }


    public byte[] getOrgData() {
        return orgData;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    public Service getService() {
        return service;
    }

    public void increasePriority() {
        highPriority = true;
    }

    public boolean isReuse() {
        return reuse;
    }

    public int getSequenceId() {
        if (sequenceId != null) {
            return sequenceId;
        } else {
            if (orgData == null) {
                throw new IllegalStateException("can't get Sequence Id from null");
            }
            return (orgData[3]);
        }

    }
}
