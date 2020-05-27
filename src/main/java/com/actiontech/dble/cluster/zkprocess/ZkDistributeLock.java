/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess;

import com.actiontech.dble.cluster.DistributeLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ZkDistributeLock extends DistributeLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDistributeLock.class);
    private InterProcessMutex distributeLock;
    private final CuratorFramework zkConn;

    public ZkDistributeLock(CuratorFramework zkConn, String path) {
        this.zkConn = zkConn;
        this.path = path;
    }

    @Override
    public boolean acquire() {
        distributeLock = new InterProcessMutex(zkConn, path);
        try {
            return distributeLock.acquire(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.warn("acquire ZkDistributeLock failed ", e);
            return false;
        }

    }

    @Override
    public void release() {
        try {
            distributeLock.release();
        } catch (Exception e) {
            LOGGER.warn("release ZkDistributeLock failed ", e);
        }
    }
}
