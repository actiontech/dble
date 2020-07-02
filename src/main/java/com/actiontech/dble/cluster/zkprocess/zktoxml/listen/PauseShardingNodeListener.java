/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PauseShardingNodeListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(PauseShardingNodeListener.class);
    private Thread waitThread;
    private final Lock lock = new ReentrantLock();
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                ChildData childData = event.getData();
                LOGGER.info("childEvent " + childData.getPath() + " " + event.getType());
                executePauseOrResume(childData);
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void executePauseOrResume(ChildData childData) throws Exception {
        String key = childData.getPath();
        String value = new String(childData.getData(), StandardCharsets.UTF_8);
        if (ClusterPathUtil.getPauseResultNodePath().equals(key)) {
            waitThread = ClusterLogic.pauseShardingNodeEvent(value, lock);
        } else if (ClusterPathUtil.getPauseResumePath().equals(key)) {
            ClusterLogic.resumeShardingNodeEvent(value, lock, waitThread);
        }
    }

    public void notifyCluster() {
        lock.lock();
        try {
            if (waitThread.isAlive()) {
                waitThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
        PauseShardingNodeManager.getInstance().resume();
    }
}
