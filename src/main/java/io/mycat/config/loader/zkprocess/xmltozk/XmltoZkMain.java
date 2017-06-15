package io.mycat.config.loader.zkprocess.xmltozk;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ZkNotifyCfg;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.listen.*;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;

public class XmltoZkMain {

    public static void main(String[] args) throws Exception {
        initFileToZK();
        System.out.println("XmltoZkMain Finished");
    }

    public static void initFileToZK() throws Exception {
        // 加载zk总服务
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();

        // 得到基本路径
        String basePath = ZKUtils.getZKBasePath();
        zkListen.setBasePath(basePath);

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
        zkListen.notify(ZkNotifyCfg.ZK_NOTIFY_LOAD_ALL.getKey());
        String confInitialized = ZKUtils.getZKBasePath() + ZookeeperPath.ZK_CONF_INITED.getKey();
        if (zkConn.checkExists().forPath(confInitialized) == null) {
            zkConn.create().creatingParentContainersIfNeeded().forPath(confInitialized);
        }
    }
}
