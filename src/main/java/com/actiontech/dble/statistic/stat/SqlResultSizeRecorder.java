/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SqlResultSizeRecorder
 */
public class SqlResultSizeRecorder {

    private ConcurrentMap<String, SqlResultSet> sqlResultSetMap = new ConcurrentHashMap<>();

    public void addSql(String sql, long resultSetSize) {
        SqlResultSet sqlResultSet;
        SqlParser sqlParserHigh = new SqlParser();
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

    static class SqlParser {

        public String fixSql(String sql) {
            if (sql != null)
                return sql.replace("\n", " ");
            return sql;
        }

        public String mergeSql(String sql) {

            String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
            return fixSql(newSql);
        }

    }

}
