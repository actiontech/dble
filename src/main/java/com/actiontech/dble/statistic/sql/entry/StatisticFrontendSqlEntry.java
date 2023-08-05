/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.entry;

import java.util.ArrayList;

public class StatisticFrontendSqlEntry extends StatisticEntry {

    private String schema;
    private long txId;
    private long examinedRows;
    private long netOutBytes;
    private long resultSize;
    private long startTimeMs;
    private long endTimeMs;
    private ArrayList<String> tableList;

    public StatisticFrontendSqlEntry(FrontendInfo frontendInfo, long startTime, long startTimeMs,
                                     String schema, String sql, int sqlType, long txId, long examinedRows, long rows,
                                     long netOutBytes, long resultSize, long endTime, long endTimeMs, ArrayList<String> tableList) {
        super(frontendInfo, startTime, sql, sqlType, rows, endTime);
        this.schema = schema;
        this.txId = txId;
        this.examinedRows = examinedRows;
        this.netOutBytes = netOutBytes;
        this.resultSize = resultSize;
        this.startTimeMs = startTimeMs;
        this.endTimeMs = endTimeMs;
        this.tableList = tableList;
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

    public ArrayList<String> getTables() {
        return tableList;
    }
}
