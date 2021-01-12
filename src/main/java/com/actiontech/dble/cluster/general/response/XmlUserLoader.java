/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;

public class XmlUserLoader implements ClusterXmlLoader {

    public XmlUserLoader(ClusterClearKeyListener confListener) {
        confListener.addChild(this, ClusterPathUtil.getUserConfPath());
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        ClusterLogic.syncUserJson(configValue);
    }

    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.syncUserXmlToCluster();
    }
}
