/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model;

import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.util.DateUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.Properties;

import static com.actiontech.dble.cluster.ClusterController.CONFIG_MODE_ZK;

public final class ClusterConfig {
    private static final ClusterConfig INSTANCE = new ClusterConfig();
    private ProblemReporter problemReporter = StartProblemReporter.getInstance();
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
    private String clusterId = null;
    private boolean needSyncHa = false;
    private int sequenceHandlerType = SEQUENCE_HANDLER_LOCAL_TIME;
    private long showBinlogStatusTimeout = 60 * 1000;
    private String sequenceStartTime;
    private boolean sequenceInstanceByZk = true;

    private long startTimeMilliseconds = 1288834974657L; //Thu Nov 04 09:42:54 CST 2010

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

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
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
            problemReporter.warn("showBinlogStatusTimeout value is " + showBinlogStatusTimeout + ", you can use default value:" + this.showBinlogStatusTimeout);
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
            problemReporter.warn("sequenceHandlerType value is " + sequenceHandlerType + ", it will use default value:" + this.sequenceHandlerType);
        }
    }

    public boolean isSequenceInstanceByZk() {
        return sequenceInstanceByZk;
    }

    @SuppressWarnings("unused")
    public void setSequenceInstanceByZk(boolean sequenceInstanceByZk) {
        this.sequenceInstanceByZk = sequenceInstanceByZk;
    }


    public String getSequenceStartTime() {
        return sequenceStartTime;
    }

    @SuppressWarnings("unused")
    public void setSequenceStartTime(String sequenceStartTime) {
        if (!StringUtil.isEmpty(sequenceStartTime)) {
            long startMilliseconds = 0;
            try {
                startMilliseconds = DateUtil.parseDate(sequenceStartTime).getTime();
            } catch (IllegalArgumentException e) {
                problemReporter.warn("sequenceStartTime in cluster.cnf invalid format, you can use default value 2010-11-04 09:42:54");
            }
            if (startMilliseconds > System.currentTimeMillis()) {
                problemReporter.warn("sequenceStartTime in cluster.cnf mustn't be over than dble start time, you can use default value 2010-11-04 09:42:54");
            }
            this.startTimeMilliseconds = startMilliseconds;
        }
        this.sequenceStartTime = sequenceStartTime;
    }

    public long sequenceStartTime() {
        return startTimeMilliseconds;
    }

    @Override
    public String toString() {
        return "ClusterConfig [" +
                "clusterEnable=" + clusterEnable +
                ", clusterMode=" + clusterMode +
                ", ipAddress=" + clusterIP +
                ", port=" + clusterPort +
                ", rootPath=" + rootPath +
                ", clusterId=" + clusterId +
                ", needSyncHa=" + needSyncHa +
                ", showBinlogStatusTimeout=" + showBinlogStatusTimeout +
                ", sequenceHandlerType=" + sequenceHandlerType +
                ", sequenceStartTime=" + sequenceStartTime +
                ", sequenceInstanceByZk=" + sequenceInstanceByZk +
                "]";
    }

    public Properties toProperties() {
        String strJson = new Gson().toJson(ClusterConfig.getInstance());
        Properties props = new Properties();
        JsonObject jsonObject = new JsonParser().parse(strJson).getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            if (key.equals("problemReporter") || key.equals("startTimeMilliseconds")) {
                continue;
            }
            props.put(key, entry.getValue().getAsString());

        }
        return props;
    }

    public boolean useZkMode() {
        return CONFIG_MODE_ZK.equals(ClusterConfig.getInstance().getClusterMode());
    }
}
