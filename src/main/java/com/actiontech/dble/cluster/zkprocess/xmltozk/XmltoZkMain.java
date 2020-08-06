/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.xmltozk.listen.*;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;

public final class XmltoZkMain {
    private XmltoZkMain() {
    }

    public static void main(String[] args) {
        try {
            ClusterController.loadClusterProperties();
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

        XmlProcessBase xmlProcess = new XmlProcessBase();

        new DbXmlToZkLoader(zkListen, xmlProcess);

        new ShardingXmlToZKLoader(zkListen, xmlProcess);

        new UserXmlToZkLoader(zkListen, xmlProcess);

        new SequenceToZkLoader(zkListen);

        new OtherMsgTozkLoader(zkListen);

        new DbGroupStatusToZkLoader(zkListen);

        xmlProcess.initJaxbClass();

        zkListen.initAllNode();
        zkListen.clearInited();
        String confInited = KVPathUtil.getConfInitedPath();
        if (ZKUtils.getConnection().checkExists().forPath(confInited) == null) {
            ZKUtils.getConnection().create().creatingParentContainersIfNeeded().forPath(confInited);
        }
    }
}
