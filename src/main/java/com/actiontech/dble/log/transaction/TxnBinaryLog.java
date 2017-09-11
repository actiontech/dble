/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.transaction;

public class TxnBinaryLog {
    private String user;
    private String host;
    private String schema;
    private long xid;
    private String executeTime;
    private String query;
    private long connid;

    public long getConnid() {
        return connid;
    }

    public void setConnid(long connid) {
        this.connid = connid;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public String getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(String executeTime) {
        this.executeTime = executeTime;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @Override
    public String toString() {
        String builder = executeTime + ", ConnID:" + connid + ", XID:" + xid + ", " +
                "MySQL user " + "'" + user +
                "'" + "@'" + host + "', " + " Current schema `" + schema +
                "`, " + "Current query " + "\n" + query + "\n";
        return builder;
    }
}
