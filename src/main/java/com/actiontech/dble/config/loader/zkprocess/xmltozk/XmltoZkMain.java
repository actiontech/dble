/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.xmltozk;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.xmltozk.listen.*;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

public final class XmltoZkMain {
    private XmltoZkMain() {
    }

    public static void main(String[] args) throws Exception {
        ClusterController.initFromShellZK();
        initFileToZK();
        System.out.println("XmltoZkMain Finished");
    }

    public static void rollbackConf() throws Exception {
        CuratorFramework zkConn = ZKUtils.getConnection();
        ConfStatus status = new ConfStatus(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ConfStatus.Status.ROLLBACK, null);
        zkConn.setData().forPath(KVPathUtil.getConfStatusPath(), status.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void writeConfFileToZK(boolean isAll, final int allMode) throws Exception {
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        CuratorFramework zkConn = ZKUtils.getConnection();

        XmlProcessBase xmlProcess = new XmlProcessBase();

        // xmltozk for schema
        new SchemasxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // xmltozk for server
        new ServerxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // xmltozk for rule
        new RulesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        xmlProcess.initJaxbClass();

        zkListen.initAllNode();
        zkListen.clearInited();
        //write flag
        ConfStatus status = new ConfStatus(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                                           isAll ? ConfStatus.Status.RELOAD_ALL : ConfStatus.Status.RELOAD,
                                           isAll ? String.valueOf(allMode) : null);
        zkConn.setData().forPath(KVPathUtil.getConfStatusPath(), status.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void initFileToZK() throws Exception {
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        CuratorFramework zkConn = ZKUtils.getConnection();

        XmlProcessBase xmlProcess = new XmlProcessBase();

        new SchemasxmlTozkLoader(zkListen, zkConn, xmlProcess);

        new ServerxmlTozkLoader(zkListen, zkConn, xmlProcess);

        new RulesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        new SequenceTozkLoader(zkListen, zkConn);

        new EcachesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        new OthermsgTozkLoader(zkListen, zkConn);

        xmlProcess.initJaxbClass();

        zkListen.initAllNode();
        zkListen.clearInited();
        String confInited = KVPathUtil.getConfInitedPath();
        if (zkConn.checkExists().forPath(confInited) == null) {
            zkConn.create().creatingParentContainersIfNeeded().forPath(confInited);
        }
    }
}
