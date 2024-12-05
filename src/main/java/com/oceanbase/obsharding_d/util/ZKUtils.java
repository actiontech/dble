/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;


import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class ZKUtils {
    private ZKUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);
    private static CuratorFramework curatorFramework = null;
    private static Map<ListenerContext, PathChildrenCache> caches = Maps.newConcurrentMap();

    static {
        try {
            curatorFramework = createConnection();
        } catch (RuntimeException e) {
            e.printStackTrace();
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

    public static Map<ListenerContext, PathChildrenCache> getCaches() {
        return caches;
    }

    public static void recreateCaches() {
        try {
            final Map<ListenerContext, PathChildrenCache> oldCaches = ZKUtils.caches;
            ZKUtils.caches = new ConcurrentHashMap<>();
            oldCaches.forEach((key, value) -> {
                addChildPathCache(key.getPath(), key.getListener());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void recreateConnection() {
        try {
            curatorFramework.close();
            curatorFramework = createConnection();
        } catch (RuntimeException e) {
            LOGGER.error("create zk connection error" + e);
            throw e;
        }
    }

    public static CuratorFramework getConnection() {
        return curatorFramework;
    }

    private static CuratorFramework createConnection() {
        String url = ClusterConfig.getInstance().getClusterIP();
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
            LOGGER.info("CuratorFramework createConnection error", ignored);
            Thread.currentThread().interrupt();
        }
        // fail situation
        framework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }

    public static PathChildrenCache addChildPathCache(String path, AbstractGeneralListener listener) {
        try {
            //watch the child status
            listener.onInit();
            final PathChildrenCache childrenCache = new PathChildrenCache(getConnection(), path, true);

            childrenCache.getListenable().addListener(listener);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

            caches.put(new ListenerContext(path, listener), childrenCache);
            return childrenCache;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static void createTempNode(String parent, String node, byte[] data) throws Exception {
        String path = ZKPaths.makePath(parent, node);
        createTempNode(path, data);
    }

    public static void createTempNode(String path, byte[] data) throws Exception {
        curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
    }


    public static class ListenerContext {
        private String path;
        private AbstractGeneralListener listener;

        public ListenerContext(String path, AbstractGeneralListener listener) {
            this.path = path;
            this.listener = listener;
        }

        public String getPath() {
            return path;
        }

        public AbstractGeneralListener getListener() {
            return listener;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ListenerContext)) return false;
            ListenerContext that = (ListenerContext) o;
            return Objects.equals(path, that.path) && Objects.equals(listener, that.listener);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, listener);
        }
    }
}
