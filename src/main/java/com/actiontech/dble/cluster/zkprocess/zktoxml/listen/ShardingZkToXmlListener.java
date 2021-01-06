/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;

public class ShardingZkToXmlListener implements NotifyService {

    public ShardingZkToXmlListener(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        KvBean configValue = ClusterHelper.getKV(ClusterPathUtil.getConfShardingPath());
        if (configValue == null) {
            throw new RuntimeException(ClusterPathUtil.getConfShardingPath() + " is null");
        }
        ClusterLogic.syncShardingJson(configValue);
        return true;
    }


}
