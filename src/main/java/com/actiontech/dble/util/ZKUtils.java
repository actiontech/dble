/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;


import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class ZKUtils {
    private ZKUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);
    private static CuratorFramework curatorFramework = null;

    static {
        try {
            curatorFramework = createConnection();
        } catch (RuntimeException e) {
            System.exit(-1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (curatorFramework != null)
                    curatorFramework.close();
            }
        }));
    }

    public static CuratorFramework getConnection() {
        return curatorFramework;
    }

    private static CuratorFramework createConnection() {
        String url = ZkConfig.getInstance().getZkURL();
        CuratorFramework framework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));
        // start connection
        framework.start();
        // wait 3 second to establish connect
        try {
            framework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (framework.getZookeeperClient().isConnected()) {
                LOGGER.info("CuratorFramework createConnection success");
                return framework;
            }
        } catch (InterruptedException ignored) {
            LOGGER.warn("CuratorFramework createConnection error", ignored);
            Thread.currentThread().interrupt();
        }
        // fail situation
        framework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }

    public static void addChildPathCache(String path, PathChildrenCacheListener listener) {
        try {
            //watch the child status
            final PathChildrenCache childrenCache = new PathChildrenCache(getConnection(), path, true);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(listener);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTempNode(String parent, String node) throws Exception {
        String path = ZKPaths.makePath(parent, node);
        createTempNode(path);
    }

    public static void createTempNode(String parent, String node, byte[] data) throws Exception {
        String path = ZKPaths.makePath(parent, node);
        curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
    }

    public static void createTempNode(String path) throws Exception {
        curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
    }
}
