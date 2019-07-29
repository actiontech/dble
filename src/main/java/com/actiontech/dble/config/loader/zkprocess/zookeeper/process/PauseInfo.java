/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by szf on 2018/4/24.
 */
public class PauseInfo {

    public static final String PAUSE = "PAUSE";
    public static final String RESUME = "RESUME";

    private String from;
    private String dataNodes;


    public int getQueueLimit() {
        return queueLimit;
    }

    public void setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
    }

    private int connectionTimeOut;


    private String type;

    private String split = ";";

    private int queueLimit;


    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(String dataNodes) {
        this.dataNodes = dataNodes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getConnectionTimeOut() {
        return connectionTimeOut;
    }

    public void setConnectionTimeOut(int connectionTimeOut) {
        this.connectionTimeOut = connectionTimeOut;
    }


    public PauseInfo(String from, String dataNodes, String type, int connectionTimeOut, int queueLimit) {
        this.dataNodes = dataNodes;
        this.from = from;
        this.type = type;
        this.connectionTimeOut = connectionTimeOut;
        this.queueLimit = queueLimit;
    }

    public PauseInfo(String value) {
        String[] s = value.split(split);
        this.from = s[0];
        this.type = s[1];
        this.dataNodes = s[2];
        this.connectionTimeOut = Integer.parseInt(s[3]);
        this.queueLimit = Integer.parseInt(s[4]);
    }

    public String toString() {
        return from + split + type + split + dataNodes + split + connectionTimeOut + split + queueLimit;
    }
}
