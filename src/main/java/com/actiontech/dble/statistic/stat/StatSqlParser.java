/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

class StatSqlParser {

    public String fixSql(String sql) {
        if (sql != null) {
            if (sql.length() > 1024) {
                sql = sql.substring(0, 1024) + "...";
            }
            return sql.replace("\n", " ");
        }

        return sql;
    }

    public String mergeSql(String sql) {

        String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
        return fixSql(newSql);
    }

}
