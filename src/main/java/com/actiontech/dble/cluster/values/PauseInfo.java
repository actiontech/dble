/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

/**
 * Created by szf on 2018/4/24.
 */
public class PauseInfo {

    public static final String PAUSE = "PAUSE";
    public static final String RESUME = "RESUME";

    private String from;
    private String shardingNodes;


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

    public String getShardingNodes() {
        return shardingNodes;
    }

    public void setShardingNodes(String shardingNodes) {
        this.shardingNodes = shardingNodes;
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


    public PauseInfo(String from, String shardingNodes, String type, int connectionTimeOut, int queueLimit) {
        this.shardingNodes = shardingNodes;
        this.from = from;
        this.type = type;
        this.connectionTimeOut = connectionTimeOut;
        this.queueLimit = queueLimit;
    }

    public PauseInfo(String value) {
        String[] s = value.split(split);
        this.from = s[0];
        this.type = s[1];
        this.shardingNodes = s[2];
        this.connectionTimeOut = Integer.parseInt(s[3]);
        this.queueLimit = Integer.parseInt(s[4]);
    }

    public String toString() {
        return from + split + type + split + shardingNodes + split + connectionTimeOut + split + queueLimit;
    }
}
