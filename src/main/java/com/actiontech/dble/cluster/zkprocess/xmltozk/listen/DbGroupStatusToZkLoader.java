/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;

/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupStatusToZkLoader implements NotifyService {

    public DbGroupStatusToZkLoader(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {
        ClusterDelayProvider.delayBeforeUploadHa();
        ClusterLogic.forHA().syncDbGroupStatusToCluster();
    }
}
