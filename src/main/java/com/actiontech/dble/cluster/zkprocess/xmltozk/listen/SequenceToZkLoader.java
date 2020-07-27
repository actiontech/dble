/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;

public class SequenceToZkLoader implements NotifyService {
    public SequenceToZkLoader(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ClusterLogic.syncSequenceToCluster();
        return true;
    }
}
