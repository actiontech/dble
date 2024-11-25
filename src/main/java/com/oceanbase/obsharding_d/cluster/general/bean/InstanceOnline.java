/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.bean;

import com.oceanbase.obsharding_d.cluster.values.OnlineType;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.util.NetUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * Created by szf on 2019/8/28.
 */
public final class InstanceOnline {

    private static final InstanceOnline INSTANCE = new InstanceOnline();
    private static final String SERVER_PORT = "SERVER_PORT";
    private static final String HOST_ADDR = "HOST_ADDR";
    private static final String START_TIME = "START_TIME";
    private final int serverPort;
    private String hostAddr;
    private final long startTime;

    private InstanceOnline() {
        serverPort = SystemConfig.getInstance().getServerPort();
        startTime = System.currentTimeMillis();
        hostAddr = NetUtil.getHostIp();
    }

    public static InstanceOnline getInstance() {
        return INSTANCE;
    }

    public boolean canRemovePath(OnlineType that) {
        if (hostAddr == null) {
            return false;
        }
        try {
            return serverPort == that.getServerPort() && Objects.equals(hostAddr, that.getHostAddr());
        } catch (Exception e) {
            //remove the old online timestamp when upgrade from old version
            return true;
        }
    }

    @Override
    public String toString() {
        JsonObject online = new JsonObject();
        online.addProperty(SERVER_PORT, serverPort);
        online.addProperty(HOST_ADDR, hostAddr);
        online.addProperty(START_TIME, startTime);
        return (new Gson()).toJson(online);
    }

}
