package io.mycat.config.loader.zkprocess.xmltozk;

import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.listen.*;
import io.mycat.config.loader.zkprocess.zookeeper.process.ConfStatus;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

public final class XmltoZkMain {
    private XmltoZkMain() {
    }

    public static void main(String[] args) throws Exception {
        initFileToZK();
        System.out.println("XmltoZkMain Finished");
    }

    public static void rollbackConf() throws Exception {
        CuratorFramework zkConn = ZKUtils.getConnection();
        ConfStatus status = new ConfStatus(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), ConfStatus.Status.ROLLBACK);
        zkConn.setData().forPath(KVPathUtil.getConfStatusPath(), status.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void writeConfFileToZK(boolean isAll) throws Exception {
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
        ConfStatus status = new ConfStatus(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), isAll ? ConfStatus.Status.RELOAD_ALL : ConfStatus.Status.RELOAD);
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
