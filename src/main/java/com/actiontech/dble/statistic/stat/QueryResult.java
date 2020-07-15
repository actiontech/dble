/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.route.util.RouterUtil;

/**
 * QueryResult
 *
 * @author zhuam
 */
public class QueryResult {

    private UserName user;
    private int sqlType;
    private String sql;
    private long sqlRows;
    private long netInBytes;
    private long netOutBytes;
    private long startTime;
    private long endTime;
    private long resultSize;

    public QueryResult(UserName user, int sqlType, String sql, long sqlRows,
                       long netInBytes, long netOutBytes, long startTime, long endTime, long resultSize) {
        super();
        this.user = user;
        this.sqlType = sqlType;
        this.sql = RouterUtil.getFixedSql(sql);
        this.sqlRows = sqlRows;
        this.netInBytes = netInBytes;
        this.netOutBytes = netOutBytes;
        this.startTime = startTime;
        this.endTime = endTime;
        this.resultSize = resultSize;
    }

    public UserName getUser() {
        return user;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getSql() {
        return sql;
    }

    public long getSqlRows() {
        return sqlRows;
    }

    public long getNetInBytes() {
        return netInBytes;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getResultSize() {
        return resultSize;
    }

}
