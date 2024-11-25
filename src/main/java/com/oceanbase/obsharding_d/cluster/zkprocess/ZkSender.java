/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess;

import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.ClusterSender;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.general.bean.KvBean;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.OnlineType;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.oceanbase.obsharding_d.cluster.zkprocess.xmltozk.listen.*;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.route.sequence.handler.IncrSequenceZKHandler;
import com.oceanbase.obsharding_d.singleton.OnlineStatus;
import com.oceanbase.obsharding_d.singleton.SequenceManager;
import com.oceanbase.obsharding_d.util.ZKUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZkSender implements ClusterSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSender.class);
    private volatile boolean connectionDetached = false;

    /**
     * only first init Lazy load
     *
     * @throws Exception
     */
    @Override
    public void initCluster() throws Exception {
        try {
            if (!ClusterConfig.getInstance().isInitZkFirst()) {
                ZktoXmlMain.loadZkListen();
            }
        } catch (Exception e) {
            LOGGER.error("error:", e);
            throw e;
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
                throw new RuntimeException("connect zk failure," + e.getMessage(), e);
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
            throw new RuntimeException("connect zk failure," + e.getMessage(), e);
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
            LOGGER.debug("", e);
            LOGGER.warn("delete zk path failed:" + path);
        }
    }

    @Override
    public void cleanKV(String path) {
        try {
            ZKUtils.getConnection().delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e1) {
            LOGGER.debug("", e1);
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

        // xmltozk for sharding
        new ShardingXmlToZKLoader(zkListen);
        // xmltozk for db
        new DbXmlToZkLoader(zkListen);
        // xmltozk for user
        new UserXmlToZkLoader(zkListen);
        new SequenceToZkLoader(zkListen);

        new DbGroupStatusToZkLoader(zkListen);

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
    public Map<String, OnlineType> getOnlineMap() {
        return ZktoXmlMain.getOnlineMap();
    }

    @Override
    public void forceResumePause() throws Exception {
        ZktoXmlMain.getPauseShardingNodeListener().notifyCluster();
    }


    @Override
    public void detachCluster() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            LOGGER.info("cluster detach begin stop listener");
            final Map<ZKUtils.ListenerContext, PathChildrenCache> caches = ZKUtils.getCaches();
            for (PathChildrenCache cache : caches.values()) {
                cache.close();
            }
            OnlineStatus.getInstance().shutdownClear();
            if (SequenceManager.getHandler() instanceof IncrSequenceZKHandler) {
                ((IncrSequenceZKHandler) SequenceManager.getHandler()).detach();
            }
            LOGGER.info("cluster detach begin close connection");
            ZKUtils.getConnection().close();
        }
    }

    @Override
    public void attachCluster() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            ZKUtils.recreateConnection();
            connectionDetached = false;
            try {
                ClusterHelper.isExist(ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName()));
            } catch (Exception e) {
                ZKUtils.getConnection().close();
                throw new IllegalStateException("can't connect to zk. ");
            }
            if (!OnlineStatus.getInstance().rebuildOnline()) {
                throw new IllegalStateException("can't create online information to zk. ");
            }
            LOGGER.info("cluster attach begin restart listener");
            if (SequenceManager.getHandler() instanceof IncrSequenceZKHandler) {
                ((IncrSequenceZKHandler) SequenceManager.getHandler()).attach();
            }
            ZKUtils.recreateCaches();


        }
    }

    @Override
    public boolean isDetach() {
        return connectionDetached;
    }

    @Override
    public void markDetach(boolean isConnectionDetached) {
        this.connectionDetached = isConnectionDetached;
    }
}
