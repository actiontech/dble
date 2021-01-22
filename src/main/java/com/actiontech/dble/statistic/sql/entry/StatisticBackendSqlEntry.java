package com.actiontech.dble.statistic.sql.entry;

public final class StatisticBackendSqlEntry extends StatisticEntry {
    //private String originalSql;
    private BackendInfo backend;
    private long firstEndTime = 0L;

    public StatisticBackendSqlEntry(
            FrontendInfo frontendInfo,
            String backendName, String backendHost, int backendPort, String shardingNode,
            int sqlType, String sql, long startTime) {
        super(frontendInfo, sqlType, sql, startTime);
        this.backend = new BackendInfo(backendName, backendHost, backendPort, shardingNode);
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

    public String getKey() {
        StringBuffer key = new StringBuffer();
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

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        //sb.append("originalSql='" + originalSql + "',");
        sb.append("StatisticBackendSqlEntry==>[txId='" + getTxId() + "',");
        sb.append("frontend=[user=" + getFrontend().getUser() + ",host=" + getFrontend().getHost() + "],");
        sb.append("backend=[node=" + backend.getNode() + ",name=" + backend.getName() + ",host&port=" + backend.getHost() + ":" + backend.getPort() + ",routeSql='" + getSql() + "'],");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "],");
        sb.append("rows=" + getRows() + "]");
        return sb.toString();
    }
}
