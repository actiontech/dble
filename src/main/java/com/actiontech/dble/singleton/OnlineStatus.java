/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.general.bean.InstanceOnline;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.NetUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class OnlineStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineStatus.class);
    private static final String SERVER_PORT = "SERVER_PORT";
    private static final String HOST_ADDR = "HOST_ADDR";
    private static final String START_TIME = "START_TIME";
    private volatile ClusterGeneralDistributeLock onlineLock = null;
    private volatile boolean onlineInited = false;
    private volatile boolean mainThreadTryed = false;
    private volatile int serverPort;
    private String hostAddr;
    private final long startTime;

    private OnlineStatus() {
        startTime = System.currentTimeMillis();
        hostAddr = NetUtil.getHostIp();
    }

    private static final OnlineStatus INSTANCE = new OnlineStatus();

    public static OnlineStatus getInstance() {
        return INSTANCE;
    }

    /**
     * mainThread call this function to init the online status for the first
     *
     * @return
     * @throws IOException
     */
    public synchronized boolean mainThreadInitClusterOnline() throws IOException {
        mainThreadTryed = true;
        return clusterOnlinInit();
    }


    /**
     * when the dble start with a cluster online failure,try to init again in the node listener
     *
     * @throws IOException
     */
    public void nodeListenerInitClusterOnline() throws IOException {
        if (mainThreadTryed) {
            clusterOnlinInit();
        }
    }

    /**
     * only init in first try of cluster online init
     * when the init finished the rebuild is give to ClusterOffLineListener
     *
     * @return
     * @throws IOException
     */
    public synchronized boolean clusterOnlinInit() throws IOException {
        if (onlineInited) {
            //when the first init finished  the online check & rebuild would handle by ClusterOffLineListener
            return false;
        }
        serverPort = SystemConfig.getInstance().getServerPort();
        //check if the online mark is on than delete the mark and renew it
        String oldValue = ClusterHelper.getKV(ClusterPathUtil.getOnlinePath(
                SystemConfig.getInstance().getInstanceId())).getValue();
        if (!"".equals(oldValue)) {
            if (InstanceOnline.getInstance().canRemovePath(oldValue)) {
                ClusterHelper.cleanKV(ClusterPathUtil.getOnlinePath(
                        SystemConfig.getInstance().getInstanceId()));
            } else {
                throw new IOException("Online path with other IP or serverPort exist,make sure different instance has different myid");
            }
        }
        if (onlineLock != null) {
            onlineLock.release();
        }
        onlineLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getOnlinePath(
                SystemConfig.getInstance().getInstanceId()),
                toString(), 6);
        int time = 0;
        while (!onlineLock.acquire()) {
            time++;
            if (time == 5) {
                LOGGER.warn(" onlineLock failed and have tried for 5 times, return false ");
                throw new IOException("set online status failed");
            }
            LOGGER.info("onlineLock failed, server will retry for 10 seconds later ");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
        }
        onlineInited = true;
        return true;
    }


    /**
     * only be called when the ClusterOffLineListener find the self online status is missing
     *
     * @throws IOException
     */
    public synchronized boolean rebuildOnline() {
        if (onlineInited) {
            if (onlineLock != null) {
                onlineLock.release();
            }
            onlineLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getOnlinePath(
                    SystemConfig.getInstance().getInstanceId()),
                    toString(), 6);
            int time = 0;
            while (!onlineLock.acquire()) {
                time++;
                if (time == 3) {
                    LOGGER.warn(" onlineLock failed and have tried for 3 times");
                    return false;
                }
                // rebuild is triggered by online missing ,no wait for too long
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
            Repository newViewRepository = new CKVStoreRepository();
            ProxyMeta.getInstance().getTmManager().setRepository(newViewRepository);
            Map<String, Map<String, String>> viewCreateSqlMap = newViewRepository.getViewCreateSqlMap();
            ProxyMeta.getInstance().getTmManager().reloadViewMeta(viewCreateSqlMap);
            return true;
        }
        return false;
    }


    public void shutdownClear() {
        if (onlineLock != null) {
            onlineLock.release();
            ClusterHelper.cleanKV(ClusterPathUtil.getOnlinePath(
                    SystemConfig.getInstance().getInstanceId()));
            LOGGER.info("shut down online status clear");
        }
    }

    public boolean canRemovePath(String value) {
        if (hostAddr == null) {
            return false;
        }
        try {
            JsonObject jsonObj = new JsonParser().parse(value).getAsJsonObject();
            return serverPort == jsonObj.get(SERVER_PORT).getAsLong() &&
                    hostAddr.equals(jsonObj.get(HOST_ADDR).getAsString());
        } catch (Exception e) {
            return false;
        }
    }

    public String toString() {
        JsonObject online = new JsonObject();
        online.addProperty(SERVER_PORT, serverPort);
        online.addProperty(HOST_ADDR, hostAddr);
        online.addProperty(START_TIME, startTime);
        return online.getAsString();
    }

}
