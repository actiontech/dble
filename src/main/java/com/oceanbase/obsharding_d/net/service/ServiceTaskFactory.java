/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;


import com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Used for self service task. Those tasks doesn't create by extern traffic.
 */
public class ServiceTaskFactory {
    final Service service;

    public ServiceTaskFactory(Service service) {
        this.service = service;
    }

    public static ServiceTaskFactory getInstance(Service service) {
        return new ServiceTaskFactory(service);
    }


    public InnerServiceTask createForGracefulClose(Collection<String> reasons, CloseType closeType) {
        return new CloseServiceTask(service, true, reasons, closeType);
    }

    public InnerServiceTask createForGracefulClose(String reason, CloseType closeType) {
        return new CloseServiceTask(service, true, Lists.newArrayList(reason), closeType);
    }


    public InnerServiceTask createForForceClose(Collection<String> reasons, CloseType closeType) {
        return new CloseServiceTask(service, false, reasons, closeType);
    }

    public InnerServiceTask createForForceClose(String reason, CloseType closeType) {
        return new CloseServiceTask(service, false, Lists.newArrayList(reason), closeType);
    }
}


