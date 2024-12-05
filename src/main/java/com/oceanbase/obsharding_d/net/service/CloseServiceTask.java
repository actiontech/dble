/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import org.apache.logging.log4j.util.Strings;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * @author dcy
 * Create Date: 2021-05-11
 */
public class CloseServiceTask extends InnerServiceTask {
    boolean gracefullyClose;
    Collection<String> reasons;
    int delayedTimes = 0;
    CloseType closeType;

    public CloseServiceTask(Service service, boolean gracefullyClose, Collection<String> reasons, CloseType closeType) {
        super(service);
        this.gracefullyClose = gracefullyClose;
        this.reasons = reasons;
        this.closeType = closeType;
    }

    public CloseType getCloseType() {
        return closeType;
    }

    public boolean isGracefullyClose() {
        return gracefullyClose;
    }

    public Collection<String> getReasons() {
        return reasons;
    }

    public String getReasonsStr() {
        return Strings.join(reasons, ';');
    }

    public boolean isFirst() {
        return delayedTimes == 0;
    }

    @Nonnull
    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.CLOSE;
    }

    public int getDelayedTimes() {
        return delayedTimes;
    }

    public CloseServiceTask setDelayedTimes(int timesTmp) {
        delayedTimes = timesTmp;
        return this;
    }

    @Override
    public String toString() {
        return "CloseServiceTask{" +
                "gracefullyClose=" + gracefullyClose +
                ", reasons=" + reasons +
                ", delayedTimes=" + delayedTimes +
                ", closeType=" + closeType +
                ", service=" + service +
                '}';
    }
}
