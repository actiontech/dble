/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.stat;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

class StatSqlParser {

    public String fixSql(String sql) {
        if (sql != null) {
            if (sql.length() > 1024) {
                sql = sql.substring(0, 1024) + "...";
            }
            return sql.replace("\n", " ");
        }

        return null;
    }

    public String mergeSql(String sql) {
        String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql);
        return fixSql(newSql);
    }

}
