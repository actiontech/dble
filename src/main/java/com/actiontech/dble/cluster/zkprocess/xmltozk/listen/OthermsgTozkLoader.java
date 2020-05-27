/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OthermsgTozkLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class OthermsgTozkLoader extends ZkMultiLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(OthermsgTozkLoader.class);


    public OthermsgTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        zookeeperListen.addToInit(this);

    }

    @Override
    public boolean notifyProcess() throws Exception {
        String online = ClusterPathUtil.getOnlinePath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), online);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + online + " success");

        String seqLine = KVPathUtil.getSequencesInstancePath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), seqLine);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + seqLine + " success");


        String incrSeq = KVPathUtil.getSequencesIncrPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), incrSeq);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + incrSeq + " success");

        String binlogPauseStatusPath = ClusterPathUtil.getBinlogPauseStatus();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), binlogPauseStatusPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + binlogPauseStatusPath + " success");


        String ddlPath = ClusterPathUtil.getDDLPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), ddlPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + ddlPath + " success");

        String confStatusPath = ClusterPathUtil.getConfStatusPath();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), confStatusPath);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + confStatusPath + " success");

        String lockBasePathPath = ClusterPathUtil.getLockBasePath();
        if (this.getCurator().checkExists().forPath(lockBasePathPath) == null) {
            ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), lockBasePathPath);
            LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + lockBasePathPath + " success");
        }
        return true;
    }

}
