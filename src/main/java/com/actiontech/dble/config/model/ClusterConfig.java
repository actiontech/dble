/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.actiontech.dble.cluster.ClusterController.CONFIG_MODE_ZK;

public final class ClusterConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfig.class);
    private static final ClusterConfig INSTANCE = new ClusterConfig();

    public static ClusterConfig getInstance() {
        return INSTANCE;
    }

    private ClusterConfig() {
    }

    public static final int SEQUENCE_HANDLER_MYSQL = 1;
    public static final int SEQUENCE_HANDLER_LOCAL_TIME = 2;
    public static final int SEQUENCE_HANDLER_ZK_DISTRIBUTED = 3;
    public static final int SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT = 4;


    private boolean clusterEnable = false;
    private String clusterMode = CONFIG_MODE_ZK;
    private String clusterIP;
    private int clusterPort;
    private String rootPath = null;
    private String clusterID = null;
    private boolean needSyncHa = false;
    private int sequenceHandlerType = SEQUENCE_HANDLER_LOCAL_TIME;
    private long showBinlogStatusTimeout = 60 * 1000;


    public boolean isClusterEnable() {
        return clusterEnable;
    }

    public void setClusterEnable(boolean clusterEnable) {
        this.clusterEnable = clusterEnable;
    }

    public String getClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(String clusterMode) {
        this.clusterMode = clusterMode.toLowerCase();
    }

    public String getClusterIP() {
        return clusterIP;
    }

    public void setClusterIP(String clusterIP) {
        this.clusterIP = clusterIP;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public void setClusterPort(int clusterPort) {
        this.clusterPort = clusterPort;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
    }

    public boolean isNeedSyncHa() {
        return needSyncHa;
    }

    public void setNeedSyncHa(boolean needSyncHa) {
        this.needSyncHa = needSyncHa;
    }

    public long getShowBinlogStatusTimeout() {
        return showBinlogStatusTimeout;
    }

    @SuppressWarnings("unused")
    public void setShowBinlogStatusTimeout(long showBinlogStatusTimeout) {
        if (showBinlogStatusTimeout > 0) {
            this.showBinlogStatusTimeout = showBinlogStatusTimeout;
        } else {
            LOGGER.warn("showBinlogStatusTimeout value is " + showBinlogStatusTimeout + ", it will use default value:" + this.showBinlogStatusTimeout);
        }
    }

    public int getSequenceHandlerType() {
        return sequenceHandlerType;
    }

    @SuppressWarnings("unused")
    public void setSequenceHandlerType(int sequenceHandlerType) {
        if (sequenceHandlerType >= 1 && sequenceHandlerType <= 4) {
            this.sequenceHandlerType = sequenceHandlerType;
        } else {
            LOGGER.warn("sequenceHandlerType value is " + sequenceHandlerType + ", it will use default value:" + this.sequenceHandlerType);
        }
    }

    @Override
    public String toString() {
        return "ClusterConfig [" +
                "clusterEnable=" + clusterEnable +
                ", clusterMode=" + clusterMode +
                ", ipAddress=" + clusterIP +
                ", port=" + clusterPort +
                ", rootPath=" + rootPath +
                ", clusterID=" + clusterID +
                ", needSyncHa=" + needSyncHa +
                ", sequenceHandlerType=" + sequenceHandlerType +
                ", showBinlogStatusTimeout=" + showBinlogStatusTimeout +
                "]";
    }

    public Properties toProperties() {
        String strJson = new Gson().toJson(ClusterConfig.getInstance());
        Properties props = new Properties();
        JsonObject jsonObject = new JsonParser().parse(strJson).getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            props.put(entry.getKey(), entry.getValue().toString());

        }
        return props;
    }

    public boolean isUseZK() {
        return CONFIG_MODE_ZK.equals(ClusterConfig.getInstance().getClusterMode());
    }
}
