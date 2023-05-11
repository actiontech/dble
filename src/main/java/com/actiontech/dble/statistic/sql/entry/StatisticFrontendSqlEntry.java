/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.entry;

public class StatisticFrontendSqlEntry extends StatisticEntry {

    private String schema;
    private long txId;
    private long examinedRows;
    private long netOutBytes;
    private long resultSize;
    private long startTimeMs;
    private long endTimeMs;

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, long startTime, long startTimeMs,
                                     String schema, String sql, long txId, long examinedRows, long rows,
                                     long netOutBytes, long resultSize, long endTime, long endTimeMs) {
        super(frontendInfo, startTime, sql, rows, endTime);
        this.schema = schema;
        this.txId = txId;
        this.examinedRows = examinedRows;
        this.netOutBytes = netOutBytes;
        this.resultSize = resultSize;
        this.startTimeMs = startTimeMs;
        this.endTimeMs = endTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public String getSchema() {
        return schema;
    }

    public long getTxId() {
        return txId;
    }

    public long getExaminedRows() {
        return examinedRows;
    }

    public long getNetInBytes() {
        return sql.getBytes().length;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public long getResultSize() {
        return resultSize;
    }
}
