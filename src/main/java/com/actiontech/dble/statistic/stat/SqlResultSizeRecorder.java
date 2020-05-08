/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SqlResultSizeRecorder
 */
public class SqlResultSizeRecorder {

    private ConcurrentMap<String, SqlResultSet> sqlResultSetMap = new ConcurrentHashMap<>();

    public void addSql(String sql, long resultSetSize) {
        SqlResultSet sqlResultSet;
        StatSqlParser sqlParserHigh = new StatSqlParser();
        sql = sqlParserHigh.mergeSql(sql);
        sqlResultSet = this.sqlResultSetMap.putIfAbsent(sql, new SqlResultSet(sql, resultSetSize));
        if (sqlResultSet != null) {
            sqlResultSet.count();
        }
    }


    /**
     * get big  SqlResult
     */
    public ConcurrentMap<String, SqlResultSet> getSqlResultSet() {

        return sqlResultSetMap;
    }


    public void clearSqlResultSet() {
        sqlResultSetMap.clear();
    }
}
