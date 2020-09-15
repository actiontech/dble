package com.actiontech.dble.backend.datasource;


import java.util.List;

public abstract class AbstractLoadBalancer implements LoadBalancer {

    @Override
    public PhysicalDbInstance select(List<PhysicalDbInstance> okSources) {
        if (okSources.size() == 1) {
            return okSources.get(0);
        } else {
            return doSelect(okSources);
        }
    }

    protected abstract PhysicalDbInstance doSelect(List<PhysicalDbInstance> okSources);
}
