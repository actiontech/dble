/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.xmltozk.listen.*;
import com.actiontech.dble.config.loader.SystemConfigLoader;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;

public final class XmltoZkMain {
    private XmltoZkMain() {
    }

    public static void main(String[] args) {
        try {
            SystemConfigLoader.initSystemConfig();
            ClusterController.loadClusterProperties();
            ClusterGeneralConfig.initConfig();

            if (!ClusterController.CONFIG_MODE_ZK.equals(ClusterConfig.getInstance().getClusterMode())) {
                throw new RuntimeException("Cluster mode is not " + ClusterController.CONFIG_MODE_ZK);
            }
            initFileToZK();
            System.out.println("XmltoZkMain Finished");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static void initFileToZK() throws Exception {
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        new DbXmlToZkLoader(zkListen);

        new ShardingXmlToZKLoader(zkListen);

        new UserXmlToZkLoader(zkListen);

        new SequenceToZkLoader(zkListen);

        new OtherMsgTozkLoader(zkListen);

        new DbGroupStatusToZkLoader(zkListen);

        zkListen.initAllNode();
        zkListen.clearInited();
        String confInited = KVPathUtil.getConfInitedPath();
        if (ZKUtils.getConnection().checkExists().forPath(confInited) == null) {
            ZKUtils.getConnection().create().creatingParentContainersIfNeeded().forPath(confInited);
        }
    }
}
