package io.mycat.config.loader.zkprocess.zktoxml.listen;

import io.mycat.MycatServer;
import io.mycat.manager.response.ReloadConfig;
import io.mycat.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.*;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 进行从sequence加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SequenceTopropertiesLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceTopropertiesLoader.class);

    /**
     * 当前文件中的zkpath信息
    */
    private final String currZkPath;

    /**
     * 后缀名
    */
    private static final String PROPERTIES_SUFFIX = ".properties";

    /**
     * 序列配制信息
    */
    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    /**
     * db序列配制信息
     */
    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    /**
     * 分布式的序列配制
     */
    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";

    /**
     * 时间的序列配制
     */
    private static final String PROPERTIES_SEQUENCE_TIME_CONF = "sequence_time_conf";

    /**
     * 监控路径信息
    */
    private ZookeeperProcessListen zookeeperListen;

    public SequenceTopropertiesLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String sequencePath = zookeeperListen.getBasePath() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();

        currZkPath = sequencePath;
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addListen(sequencePath, this);

    }

    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {

        // 1,将集群server目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf sequenceDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey(), sequenceDirectory);

        // 取到当前根目录 信息
        sequenceDirectory = (DiretoryInf) sequenceDirectory.getSubordinateInfo().get(0);

        // 将zk序列配配制信息入本地文件
        this.sequenceZkToProperties(currZkPath, PROPERTIES_SEQUENCE_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_conf to local properties success");

        // 将zk的db方式信息入本地文件
        this.sequenceZkToProperties(currZkPath, PROPERTIES_SEQUENCE_DB_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_db_conf to local properties success");

        // 将zk的分布式信息入本地文件
        this.seqWriteOneZkToProperties(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_distributed_conf to local properties success");

        // 将zk时间序列入本地文件
        this.seqWriteOneZkToProperties(PROPERTIES_SEQUENCE_TIME_CONF, sequenceDirectory);

        LOGGER.info("SequenceTozkLoader notifyProcess sequence_time_conf to local properties success");

        LOGGER.info("SequenceTozkLoader notifyProcess xml to local properties is success");

        if (!isAll && MycatServer.getInstance().getProcessors() != null)
            ReloadConfig.reload();
        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param name schema文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void sequenceZkToProperties(String basePath, String name, DiretoryInf seqDirectory) throws Exception {
        // 读取当前节的信息
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey());

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            // 读取common目录下的数据
            ZkDataImpl commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            // 读取公共节点的信息
            ConfFileRWUtils.writeFile(commData.getName(), commData.getValue());

            String seqComm = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey();
            seqComm = seqComm + ZookeeperPath.ZK_SEPARATOR.getKey() + commData.getName();

            this.zookeeperListen.watchPath(currZkPath, seqComm);

        }

        // 集群中特有的节点的配制信息
        ZkDirectoryImpl zkClusterDir = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey());

        if (null != zkClusterDir) {

            String clusterName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);

            String nodeName = name + "-" + clusterName + PROPERTIES_SUFFIX;

            // 读取cluster目录下的数据
            ZkDataImpl clusterData = (ZkDataImpl) this.getZkData(zkClusterDir, nodeName);

            if (null != clusterData) {
                // 读取当前集群中特有的节点的信息
                ConfFileRWUtils.writeFile(clusterData.getName(), clusterData.getValue());

                String seqCluster = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey();
                seqCluster = seqCluster + ZookeeperPath.ZK_SEPARATOR.getKey() + clusterData.getName();

                this.zookeeperListen.watchPath(currZkPath, seqCluster);
            }
        }
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     * @param name schema文件的信息
     * @throws Exception 异常信息
     * @创建日期 2016年9月17日
     */
    private void seqWriteOneZkToProperties(String name, DiretoryInf seqDirectory) throws Exception {
        // 读取当前节的信息
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey());

        ZkDataImpl commData = null;

        if (null != zkDirectory) {
            String writeFile = name + PROPERTIES_SUFFIX;

            // 读取common目录下的数据
            commData = (ZkDataImpl) this.getZkData(zkDirectory, writeFile);

            // comm路径的监控路径
            String seqComm = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_COMMON.getKey();
            seqComm = seqComm + ZookeeperPath.ZK_SEPARATOR.getKey() + commData.getName();

            this.zookeeperListen.watchPath(currZkPath, seqComm);
        }

        // 集群中特有的节点的配制信息
        ZkDirectoryImpl zkClusterDir = (ZkDirectoryImpl) this.getZkDirectory(seqDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey());

        ZkDataImpl clusterData = null;

        if (null != zkClusterDir) {

            String clusterName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);

            String nodeName = name + "-" + clusterName + PROPERTIES_SUFFIX;

            // 读取cluster目录下的数据
            clusterData = (ZkDataImpl) this.getZkData(zkClusterDir, nodeName);

            if (null != clusterData) {
                // comm路径的监控路径
                String seqCluster = ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_CLUSTER.getKey();
                seqCluster = seqCluster + ZookeeperPath.ZK_SEPARATOR.getKey() + clusterData.getName();

                this.zookeeperListen.watchPath(currZkPath, seqCluster);
            }
        }

        // 如果配制了单独节点的信息,以公共的名称，写入当前的值
        if (clusterData != null && commData != null) {
            // 读取公共节点的信息
            ConfFileRWUtils.writeFile(commData.getName(), clusterData.getValue());
        } else if (commData != null) {
            // 读取当前集群中特有的节点的信息
            ConfFileRWUtils.writeFile(commData.getName(), commData.getValue());
        }
    }

}
