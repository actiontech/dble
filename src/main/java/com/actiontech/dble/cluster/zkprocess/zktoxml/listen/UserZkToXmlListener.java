/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UserZkToXmlListener implements NotifyService {
    private static final Logger LOGGER = LogManager.getLogger(UserZkToXmlListener.class);

    public UserZkToXmlListener(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {

        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        final RawJson configValue = clusterHelper.getPathValue(ClusterMetaUtil.getUserConfPath()).map(ClusterValue::getData).orElse(null);
        if (configValue == null) {
            LOGGER.warn("receive empty value");
        }
        ClusterLogic.forConfig().syncUserJson(ClusterPathUtil.getUserConfPath(), configValue);

    }


}
