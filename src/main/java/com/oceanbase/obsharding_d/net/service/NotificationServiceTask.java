/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-04-16
 */
public class NotificationServiceTask extends InnerServiceTask {
    public NotificationServiceTask(Service service) {
        super(service);
    }

    @Nonnull
    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.NOTIFICATION;
    }
}
