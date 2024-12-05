/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.xmltozk.listen;

import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.NotifyService;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.ZookeeperProcessListen;

public class SequenceToZkLoader implements NotifyService {
    public SequenceToZkLoader(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {
        ClusterLogic.forConfig().syncSequencePropsToCluster();
    }
}
