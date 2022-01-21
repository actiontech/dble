/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
