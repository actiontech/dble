package io.mycat.config.loader.zkprocess.xmltozk;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.console.ZkNotifyCfg;
import org.apache.curator.framework.CuratorFramework;

import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.xmltozk.listen.EcachesxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.OthermsgTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.RulesxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.SchemasxmlTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.SequenceTozkLoader;
import io.mycat.config.loader.zkprocess.xmltozk.listen.ServerxmlTozkLoader;
import io.mycat.util.ZKUtils;

import static io.mycat.config.loader.console.ZookeeperPath.ZK_CONF_INITED;

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
        new SequenceTozkLoader(zkListen, zkConn, xmlProcess);

        // 缓存配制信息
        new EcachesxmlTozkLoader(zkListen, zkConn, xmlProcess);

        // 将其他信息加载的zk中
        new OthermsgTozkLoader(zkListen, zkConn, xmlProcess);

        // 初始化xml转换操作
        xmlProcess.initJaxbClass();

        // 加载通知进程
        zkListen.notify(ZkNotifyCfg.ZK_NOTIFY_LOAD_ALL.getKey());
        if (MycatServer.getInstance().getProcessors() != null) {
            //Initialized flag
            String confInitialized = basePath + ZK_CONF_INITED;
            zkConn.create().creatingParentContainersIfNeeded().forPath(confInitialized);
        }
    }
}
