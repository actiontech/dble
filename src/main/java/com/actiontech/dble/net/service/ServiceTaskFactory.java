package com.actiontech.dble.net.service;


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


    public InnerServiceTask createForGracefulClose(Collection<String> reasons) {
        return new CloseServiceTask(service, true, reasons);
    }

    public InnerServiceTask createForGracefulClose(String reason) {
        return new CloseServiceTask(service, true, Lists.newArrayList(reason));
    }


    public InnerServiceTask createForForceClose(Collection<String> reasons) {
        return new CloseServiceTask(service, false, reasons);
    }

    public InnerServiceTask createForForceClose(String reason) {
        return new CloseServiceTask(service, false, Lists.newArrayList(reason));
    }
}


