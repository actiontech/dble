/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

/**
 * SqlResultSet
 */
public class SqlResultSet {
    private String sql;
    private long resultSetSize = 0;
    private int count = 1;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getResultSetSize() {
        return resultSetSize;
    }

    public void setResultSetSize(long resultSetSize) {
        this.resultSetSize = resultSetSize;
    }

    public int getCount() {
        return count;
    }

    public void count() {
        this.count++;
    }


}
