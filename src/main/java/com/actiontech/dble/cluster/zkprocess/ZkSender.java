/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.ClusterSender;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.xmltozk.listen.*;
import com.actiontech.dble.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.ZKUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZkSender implements ClusterSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSender.class);

    @Override
    public void initCluster() {
        try {
            ZktoXmlMain.loadZkToFile();
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
    }

    @Override
    public void setKV(String path, String value) throws Exception {
        ZKUtils.getConnection().create().orSetData().creatingParentsIfNeeded().forPath(path, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public KvBean getKV(String path) {
        try {
            byte[] data = ZKUtils.getConnection().getData().forPath(path);
            if (data == null) {
                return null;
            } else {
                return new KvBean(path, new String(data, StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            if (e instanceof KeeperException.NoNodeException) {
                return null;
            } else {
                throw new RuntimeException("connect zk failure", e);
            }
        }
    }

    @Override
    public List<KvBean> getKVPath(String path) {
        if (path.endsWith(ClusterPathUtil.SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        try {
            List<String> childList = ZKUtils.getConnection().getChildren().forPath(path);
            if (childList == null) {
                return new ArrayList<>(0);
            }
            List<KvBean> allList = new ArrayList<>(childList.size());
            for (String key : childList) {
                KvBean bean = getKV(path + ClusterPathUtil.SEPARATOR + key);
                if (bean != null) {
                    allList.add(bean);
                }
            }
            return allList;
        } catch (Exception e) {
            throw new RuntimeException("connect zk failure", e);
        }
    }

    @Override
    public void cleanPath(String path) {
        try {
            if (path.endsWith(ClusterPathUtil.SEPARATOR)) {
                path = path.substring(0, path.length() - 1);
            }
            ZKUtils.getConnection().delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            LOGGER.warn("delete zk path failed:" + path);
        }
    }

    @Override
    public void cleanKV(String path) {
        try {
            ZKUtils.getConnection().delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e1) {
            throw new RuntimeException("cleanKV failure for" + path);
        }
    }

    @Override
    public void createSelfTempNode(String path, String value) throws Exception {
        ZKUtils.createTempNode(path, SystemConfig.getInstance().getInstanceName(),
                value.getBytes(StandardCharsets.UTF_8));
        LOGGER.info("sent self status to zk, waiting other instances, path is:" + path);
    }


    @Override
    public void writeConfToCluster() throws Exception {
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // xmltozk for sharding
        new ShardingXmlToZKLoader(zkListen, xmlProcess);
        // xmltozk for db
        new DbXmlToZkLoader(zkListen, xmlProcess);
        // xmltozk for user
        new UserXmlToZkLoader(zkListen, xmlProcess);
        new SequenceToZkLoader(zkListen);

        new DbGroupStatusToZkLoader(zkListen);

        xmlProcess.initJaxbClass();
        zkListen.initAllNode();
        zkListen.clearInited();
    }

    @Override
    public DistributeLock createDistributeLock(String path, String value) {
        return new ZkDistributeLock(path, value, this);
    }

    @Override
    public DistributeLock createDistributeLock(String path, String value, int maxErrorCnt) {
        return new ZkDistributeLock(path, value, this);
    }


    @Override
    public Map<String, String> getOnlineMap() {
        return ZktoXmlMain.getOnlineMap();
    }

    @Override
    public void forceResumePause() throws Exception {
        ZktoXmlMain.getPauseShardingNodeListener().notifyCluster();
    }
}
