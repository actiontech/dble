/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.values.OriginClusterEvent;

/**
 * Created by szf on 2018/1/26.
 */
public interface ClusterXmlLoader {

    void notifyProcess(OriginClusterEvent<?> changeEvent, boolean ignoreTheGrandChild) throws Exception;

    void notifyCluster() throws Exception;
}
