package io.mycat.config.loader.zkprocess.xmltozk.listen;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ShowBinlogStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static io.mycat.manager.response.ShowBinlogStatus.BINLOG_PAUSE_INSTANCES;
import static io.mycat.manager.response.ShowBinlogStatus.BINLOG_PAUSE_STATUS;

/**
 * 其他一些信息加载到zk中
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class OthermsgTozkLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(OthermsgTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    public OthermsgTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

    }

    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {
        // 添加line目录，用作集群中节点，在线的基本目录信息
        String line = currZkPath + ZookeeperPath.FLOW_ZK_PATH_ONLINE.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), line);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + line + " success");

        // 添加序列目录信息
        String seqLine = currZkPath + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        seqLine = seqLine + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.ZK_PATH_INSTANCE.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLine);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLine + " success");

        String seqLeader = currZkPath + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        seqLeader = seqLeader + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_LEADER.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLeader);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLeader + " success");

        String incrSeq = currZkPath + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
        incrSeq = incrSeq + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), incrSeq);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + incrSeq + " success");

        String binlogPauseStatusPath = currZkPath + BINLOG_PAUSE_STATUS;
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), binlogPauseStatusPath);
        this.getCurator().setData().forPath(binlogPauseStatusPath, ShowBinlogStatus.BinlogPauseStatus.OFF.toString().getBytes(StandardCharsets.UTF_8));
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + binlogPauseStatusPath + " success");

        String binlogPauseInstances = currZkPath + BINLOG_PAUSE_INSTANCES;
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), binlogPauseInstances);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + binlogPauseInstances + " success");

        String ddlPath = currZkPath + ZookeeperPath.ZK_DDL.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), ddlPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + ddlPath + " success");

        String ddlLockPath = currZkPath + ZookeeperPath.ZK_LOCK.getKey() + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.ZK_DDL.getKey() ;
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), ddlLockPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + ddlLockPath + " success");
        return true;
    }

}
