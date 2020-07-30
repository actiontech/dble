/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OtherMsgTozkLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public class OtherMsgTozkLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(OtherMsgTozkLoader.class);


    public OtherMsgTozkLoader(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);

    }

    @Override
    public boolean notifyProcess() throws Exception {
        String online = ClusterPathUtil.getOnlinePath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), online);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + online + " success");

        String seqCommon = ClusterPathUtil.getSequencesCommonPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), seqCommon);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + seqCommon + " success");

        String seqLine = KVPathUtil.getSequencesInstancePath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), seqLine);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + seqLine + " success");


        String incrSeq = KVPathUtil.getSequencesIncrPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), incrSeq);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + incrSeq + " success");


        String ddlPath = ClusterPathUtil.getDDLPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), ddlPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + ddlPath + " success");

        String viewPath = ClusterPathUtil.getViewPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), viewPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + viewPath + " success");

        String ddlLockPath = ClusterPathUtil.getDDLLockPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), ddlLockPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + ddlLockPath + " success");

        String viewLockPath = ClusterPathUtil.getViewLockPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), viewLockPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + viewLockPath + " success");

        String confStatusPath = ClusterPathUtil.getConfStatusPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), confStatusPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + confStatusPath + " success");

        String haStatusPath = ClusterPathUtil.getHaStatusPath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), haStatusPath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + haStatusPath + " success");

        String haResponsePath = ClusterPathUtil.getHaResponsePath();
        ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), haResponsePath);
        LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + haResponsePath + " success");

        String lockBasePathPath = ClusterPathUtil.getLockBasePath();
        if (ZKUtils.getConnection().checkExists().forPath(lockBasePathPath) == null) {
            ZKPaths.mkdirs(ZKUtils.getConnection().getZookeeperClient().getZooKeeper(), lockBasePathPath);
            LOGGER.info("OtherMsgTozkLoader zookeeper mkdir " + lockBasePathPath + " success");
        }
        return true;
    }

}
