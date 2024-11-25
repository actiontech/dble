/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.entry;

import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class StatisticFrontendSqlEntry extends StatisticEntry {

    private String schema;
    private int sqlType = -99;
    private String sql;
    private volatile ConcurrentHashMap<String, StatisticBackendSqlEntry> backendSqlEntrys = new ConcurrentHashMap<>(8);
    private volatile LongAdder examinedRows = new LongAdder();
    private boolean isNeedToTx;

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, long startTime) {
        super(frontendInfo, startTime);
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
        if (null == sql) {
            return sqlType;
        }
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

    public boolean isNeedToTx() {
        return isNeedToTx;
    }

    public void setNeedToTx(boolean needToTx) {
        isNeedToTx = needToTx;
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
