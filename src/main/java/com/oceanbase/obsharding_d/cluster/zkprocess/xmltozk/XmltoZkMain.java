/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.xmltozk;

import com.oceanbase.obsharding_d.cluster.ClusterController;
import com.oceanbase.obsharding_d.cluster.ClusterGeneralConfig;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.oceanbase.obsharding_d.cluster.zkprocess.xmltozk.listen.*;
import com.oceanbase.obsharding_d.config.loader.SystemConfigLoader;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.util.KVPathUtil;
import com.oceanbase.obsharding_d.util.ZKUtils;

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
