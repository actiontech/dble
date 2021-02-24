package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.server.parser.ServerParseFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class StatisticFrontendSqlEntry extends StatisticEntry {

    private String schema;
    private int sqlType = -99;
    private String sql;
    private volatile Map<String, StatisticBackendSqlEntry> backendSqlEntrys = new HashMap<>(8);
    private volatile LongAdder examinedRows = new LongAdder();

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, long txId, long startTime) {
        super(frontendInfo, txId, startTime);
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

    public void setRowsAndExaminedRows(long rows) {
        super.rows = rows;
        examinedRows.add(rows);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StatisticFrontendSqlEntry==>[");
        sb.append("sql='" + getSql() + "',");
        sb.append("txId='" + getTxId() + "',");
        sb.append("frontend=[userId=" + getFrontend().getUserId() + ",user=" + getFrontend().getUser() + ",host&port=" + getFrontend().getHost() + ":" + getFrontend().getPort() + "]");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "],");
        sb.append("sendClientRows=" + getRows() + "]");
        return sb.toString();
    }
}
