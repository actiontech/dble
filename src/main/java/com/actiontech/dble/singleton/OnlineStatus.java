/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.DistributeLock;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class OnlineStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineStatus.class);
    private static final String SERVER_PORT = "SERVER_PORT";
    private static final String HOST_ADDR = "HOST_ADDR";
    private static final String START_TIME = "START_TIME";
    private volatile DistributeLock onlineLock = null;
    private volatile boolean onlineInited = false;
    private final int serverPort;
    private String hostAddr;
    private final long startTime;

    private OnlineStatus() {
        serverPort = DbleServer.getInstance().getConfig().getSystem().getServerPort();
        startTime = System.currentTimeMillis();
        hostAddr = getHostIp();
    }

    private static final OnlineStatus INSTANCE = new OnlineStatus();

    public static OnlineStatus getInstance() {
        return INSTANCE;
    }

    public boolean metaUcoreInit(boolean init) throws IOException {
        if (!init && !onlineInited) {
            return false;
        }
        //check if the online mark is on than delete the mark and renew it
        ClusterHelper.cleanKV(ClusterPathUtil.getOnlinePath(ClusterGeneralConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)));
        if (onlineLock != null) {
            onlineLock.release();
        }
        onlineLock = new DistributeLock(ClusterPathUtil.getOnlinePath(ClusterGeneralConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                "" + System.currentTimeMillis(), 6);
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


    public boolean canRemovePath(String value) {
        if (hostAddr == null) {
            return false;
        }
        try {
            JSONObject jsonObj = JSONObject.parseObject(value);
            return serverPort == Long.parseLong(jsonObj.getString(SERVER_PORT)) &&
                    hostAddr.equals(jsonObj.getString(HOST_ADDR));
        } catch (Exception e) {
            return false;
        }
    }

    public String toString() {
        JSONObject online = new JSONObject();
        online.put(SERVER_PORT, serverPort);
        online.put(HOST_ADDR, hostAddr);
        online.put(START_TIME, startTime);
        return online.toJSONString();
    }

    private static String getHostIp() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = (InetAddress) addresses.nextElement();
                    if (ip != null &&
                            ip instanceof Inet4Address &&
                            !ip.isLoopbackAddress() &&
                            ip.getHostAddress().indexOf(":") == -1) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

}
