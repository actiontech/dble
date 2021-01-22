package com.actiontech.dble.statistic.sql.entry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class StatisticFrontendSqlEntry extends StatisticEntry {

    private String schema;
    private volatile Map<String, StatisticBackendSqlEntry> backendSqlEntrys = new LinkedHashMap(8);
    private volatile LongAdder examinedRows = new LongAdder();

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo) {
        super(frontendInfo);
    }

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, int sqlType, long txId, long startTime) {
        super(frontendInfo, sqlType, txId, startTime);
    }

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, int sqlType, String routeSql, long startTime) {
        super(frontendInfo, sqlType, routeSql, startTime);
    }

    public LongAdder getExaminedRows() {
        return examinedRows;
    }

    public void addExaminedRows(long row) {
        examinedRows.add(row);
    }

    public void addExaminedRows() {
        examinedRows.increment();
    }

    public void put(String key, StatisticBackendSqlEntry backendSqlEntry) {
        this.backendSqlEntrys.put(key, backendSqlEntry);
    }

    public StatisticBackendSqlEntry getBackendSqlEntry(String key) {
        return this.backendSqlEntrys.get(key);
    }

    public void clear() {
        backendSqlEntrys.clear();
    }

    public Map<String, StatisticBackendSqlEntry> getBackendSqlEntrys() {
        return backendSqlEntrys;
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StatisticFrontendSqlEntry==>[");
        sb.append("sql='" + getSql() + "',");
        sb.append("txId='" + getTxId() + "',");
        sb.append("frontend=[user=" + getFrontend().getUser() + ",host&port=" + getFrontend().getHost() + ":" + getFrontend().getPort() + "]");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "],");
        sb.append("sendClientRows=" + getRows() + "]");
        return sb.toString();
    }
}
