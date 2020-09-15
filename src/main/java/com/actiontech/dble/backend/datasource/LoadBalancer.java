package com.actiontech.dble.backend.datasource;

import java.util.List;

public interface LoadBalancer {

    PhysicalDbInstance select(List<PhysicalDbInstance> okSources);
}
