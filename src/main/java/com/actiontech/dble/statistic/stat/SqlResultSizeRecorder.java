/*
 * Copyright (C) 2016-2017 ActionTech.
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


    public void addSql(String sql, int resultSetSize) {
        SqlResultSet sqlResultSet;
        SqlParser sqlParserHigh = new SqlParser();
        sql = sqlParserHigh.mergeSql(sql);
        if (this.sqlResultSetMap.containsKey(sql)) {
            sqlResultSet = this.sqlResultSetMap.get(sql);
            sqlResultSet.count();
            sqlResultSet.setSql(sql);
            System.out.println(sql);
            sqlResultSet.setResultSetSize(resultSetSize);
        } else {
            sqlResultSet = new SqlResultSet();
            sqlResultSet.setResultSetSize(resultSetSize);
            sqlResultSet.setSql(sql);
            this.sqlResultSetMap.put(sql, sqlResultSet);
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
