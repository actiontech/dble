/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.cluster.values.OriginClusterEvent;

/**
 * Created by szf on 2018/1/26.
 */
public interface ClusterXmlLoader {

    void notifyProcess(OriginClusterEvent<?> changeEvent, boolean ignoreTheGrandChild) throws Exception;

    void notifyCluster() throws Exception;
}
