/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * SqlResultSet
 */
public class SqlResultSet {
    private String sql;
    private long resultSetSize;
    private AtomicInteger count = new AtomicInteger(1);

    SqlResultSet(String sql, long resultSetSize) {
        this.sql = sql;
        this.resultSetSize = resultSetSize;
    }

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
        return this.count.get();
    }

    public void count() {
        this.count.incrementAndGet();
    }
}
