/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.datasource;

import java.util.List;

public interface LoadBalancer {

    PhysicalDbInstance select(List<PhysicalDbInstance> okSources);
}
