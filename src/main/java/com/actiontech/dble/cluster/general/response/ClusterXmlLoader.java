/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.general.bean.KvBean;

/**
 * Created by szf on 2018/1/26.
 */
public interface ClusterXmlLoader {

    void notifyProcess(KvBean configValue) throws Exception;

    void notifyCluster() throws Exception;
}
