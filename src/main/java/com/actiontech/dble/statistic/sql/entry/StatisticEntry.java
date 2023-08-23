/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.server.parser.ServerParseFactory;

public class StatisticEntry {
    private FrontendInfo frontend;
    protected long rows;
    protected String sql;
    protected int sqlType;
    protected long duration;

    public StatisticEntry(FrontendInfo frontendInfo, long startTime,
                          String sql, int sqlType, long rows, long endTime) {
        this.frontend = frontendInfo;
        this.sql = sql;
        this.sqlType = sqlType;
        this.rows = rows;
        this.duration = endTime - startTime;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public String getSql() {
        return sql;
    }

    public int getSqlType() {
        if (sqlType == -99 && sql != null) {
            this.sqlType = ServerParseFactory.getShardingParser().parse(sql) & 0xff;
        }
        return sqlType;
    }

    public FrontendInfo getFrontend() {
        return frontend;
    }

    public long getDuration() {
        return duration;
    }

    public long getDurationMs() {
        return duration / 1000000;
    }

}
