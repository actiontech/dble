/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.entry;

import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;

public final class StatisticBackendSqlEntry extends StatisticEntry {
    //private String originalSql;
    private BackendInfo backend;
    private long firstEndTime = 0L;
    private int sqlType = -99;
    private String sql;
    private boolean isNeedToTx;
    private boolean isEnd = false;

    public StatisticBackendSqlEntry(
            FrontendInfo frontendInfo,
            String backendName, String backendHost, int backendPort, String shardingNode,
            int sqlType, String sql, long startTime) {
        super(frontendInfo, startTime);
        this.backend = new BackendInfo(backendName, backendHost, backendPort, shardingNode);
        this.sqlType = sqlType;
        this.sql = sql;
    }

    public StatisticBackendSqlEntry(
            FrontendInfo frontendInfo,
            String backendName, String backendHost, int backendPort, String shardingNode, String sql, long startTime) {
        super(frontendInfo, startTime);
        this.backend = new BackendInfo(backendName, backendHost, backendPort, shardingNode);
        this.sql = sql;
    }

    public BackendInfo getBackend() {
        return backend;
    }

    public long getFirstEndTime() {
        return firstEndTime;
    }

    public void setFirstEndTime(long firstEndTime) {
        this.firstEndTime = firstEndTime;
    }

    public int getSqlType() {
        if (sqlType == -99) {
            this.sqlType = ServerParseFactory.getShardingParser().parse(sql) & 0xff;
        }
        return sqlType;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql.replaceAll("[\\t\\n\\r]", " ");
    }

    public boolean isNeedToTx() {
        return isNeedToTx;
    }

    public void setNeedToTx(boolean needToTx) {
        isNeedToTx = needToTx;
    }

    public String getKey() {
        StringBuffer key = new StringBuffer();
        key.append(getFrontend().getUserId());
        key.append(":");
        key.append(getFrontend().getUser());
        key.append(":");
        key.append(getFrontend().getHost());
        key.append("|");
        key.append(getBackend().getNode());
        key.append(":");
        key.append(getBackend().getName());
        key.append(":");
        key.append(getBackend().getHost());
        key.append(":");
        key.append(getBackend().getPort());
        return key.toString();
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StatisticBackendSqlEntry==>[txId='" + getTxId() + "',");
        sb.append("frontend=[userId=" + getFrontend().getUserId() + ",user=" + getFrontend().getUser() + ",host=" + getFrontend().getHost() + "],");
        sb.append("backend=[node=" + backend.getNode() + ",name=" + backend.getName() + ",host&port=" + backend.getHost() + ":" + backend.getPort() + ",routeSql='" + getSql() + "'],");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "],");
        sb.append("rows=" + getRows() + "]");
        return sb.toString();
    }
}
