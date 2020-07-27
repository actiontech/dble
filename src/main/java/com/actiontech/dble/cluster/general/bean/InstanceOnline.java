package com.actiontech.dble.cluster.general.bean;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.NetUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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

    public boolean canRemovePath(String value) {
        if (hostAddr == null) {
            return false;
        }
        try {
            JsonObject jsonObj = new JsonParser().parse(value).getAsJsonObject();
            return serverPort == jsonObj.get(SERVER_PORT).getAsLong() &&
                    hostAddr.equals(jsonObj.get(HOST_ADDR).getAsString());
        } catch (Exception e) {
            //remove the old online timestamp when upgrade from old version
            return true;
        }
    }

    public String toString() {
        JsonObject online = new JsonObject();
        online.addProperty(SERVER_PORT, serverPort);
        online.addProperty(HOST_ADDR, hostAddr);
        online.addProperty(START_TIME, startTime);
        return (new Gson()).toJson(online);
    }

}
