/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * @author dcy
 * Create Date: 2021-05-11
 */
public class CloseServiceTask extends InnerServiceTask {
    boolean gracefullyClose;
    Collection<String> reasons;

    public CloseServiceTask(Service service, boolean gracefullyClose, Collection<String> reasons) {
        super(service);
        this.gracefullyClose = gracefullyClose;
        this.reasons = reasons;
    }

    public boolean isGracefullyClose() {
        return gracefullyClose;
    }

    public Collection<String> getReasons() {
        return reasons;
    }

    @Nonnull
    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.CLOSE;
    }

    @Override
    public String toString() {
        return "CloseServiceTask{" +
                "gracefullyClose=" + gracefullyClose +
                ", reasons=" + reasons +
                ", service=" + service +
                '}';
    }
}