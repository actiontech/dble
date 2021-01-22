package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.server.parser.ServerParseFactory;

public class StatisticEntry {
    private FrontendInfo frontend;
    private int sqlType = -99;
    private long rows = 0L;
    private long startTime = 0L;
    private long allEndTime = 0L;
    private long txId = -1L;
    private String sql;

    public StatisticEntry(FrontendInfo frontendInfo) {
        this.frontend = frontendInfo;
    }

    public StatisticEntry(FrontendInfo frontendInfo, int sqlType, String sql) {
        this.frontend = frontendInfo;
        this.sqlType = sqlType;
        this.sql = sql.replaceAll("[\\t\\n\\r]", " ");
    }

    public StatisticEntry(FrontendInfo frontendInfo, int sqlType, long txId, long startTime) {
        this.frontend = frontendInfo;
        this.sqlType = sqlType;
        this.txId = txId;
        this.startTime = startTime;
    }

    public StatisticEntry(FrontendInfo frontendInfo, int sqlType, String sql, long startTime) {
        this.frontend = frontendInfo;
        this.sqlType = sqlType;
        this.sql = sql.replaceAll("[\\t\\n\\r]", " ");
        this.startTime = startTime;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public long getTxId() {
        return txId;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public void addRows() {
        this.rows += 1;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getAllEndTime() {
        return allEndTime;
    }

    public void setAllEndTime(long allEndTime) {
        this.allEndTime = allEndTime;
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

    public FrontendInfo getFrontend() {
        return frontend;
    }

    public long getDuration() {
        return allEndTime - startTime;
    }

    public static class BackendInfo {
        String name; // db_instance
        String host;
        int port;
        String node; // sharding_node

        BackendInfo(String name, String host, int port, String node) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.node = node;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getNode() {
            return node;
        }
    }
}
