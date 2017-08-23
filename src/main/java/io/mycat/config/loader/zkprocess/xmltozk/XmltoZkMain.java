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
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        // 获得zk的连接信息
        CuratorFramework zkConn = ZKUtils.getConnection();

        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 进行xmltozk的schema文件的操作
        new SchemasxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行xmltozk的server文件的操作
        new ServerxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行rule文件到zk的操作
        new RulesxmlTozkLoader(zkListen, zkConn, xmlProcess);
        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 加载通知进程
        zkListen.initAllNode();
        zkListen.clearInited();
        //write flag
        ConfStatus status = new ConfStatus(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), isAll ? ConfStatus.Status.RELOAD_ALL : ConfStatus.Status.RELOAD);
        zkConn.setData().forPath(KVPathUtil.getConfStatusPath(), status.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static void initFileToZK() throws Exception {
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        // 获得zk的连接信息
        CuratorFramework zkConn = ZKUtils.getConnection();

        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // 进行xmltozk的schema文件的操作
        new SchemasxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行xmltozk的server文件的操作
        new ServerxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行rule文件到zk的操作
        new RulesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 进行序列信息入zk中
        new SequenceTozkLoader(zkListen, zkConn);

        // 缓存配制信息
        new EcachesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 将其他信息加载的zk中
        new OthermsgTozkLoader(zkListen, zkConn);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 加载通知进程
        zkListen.initAllNode();
        zkListen.clearInited();
        String confInited = KVPathUtil.getConfInitedPath();
        if (zkConn.checkExists().forPath(confInited) == null) {
            zkConn.create().creatingParentContainersIfNeeded().forPath(confInited);
        }
    }
}
