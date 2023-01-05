/*
 * Copyright (C) 2016-2023 ActionTech.
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

public class DbGroupsZKToXmlListener implements NotifyService {
    private static final Logger LOGGER = LogManager.getLogger(DbGroupsZKToXmlListener.class);

    public DbGroupsZKToXmlListener(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {
        final String path = ClusterPathUtil.getDbConfPath();
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        final RawJson configValue = clusterHelper.getPathValue(ClusterMetaUtil.getDbConfPath()).map(ClusterValue::getData).orElse(null);
        if (configValue == null) {
            LOGGER.warn("receive empty value");
        }
        ClusterLogic.forConfig().syncDbJson(path, configValue);
    }


}
