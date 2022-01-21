/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

import java.util.List;

public interface LoadBalancer {

    PhysicalDbInstance select(List<PhysicalDbInstance> okSources);
}
