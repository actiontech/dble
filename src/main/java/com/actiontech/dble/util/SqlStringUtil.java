/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.server.parser.DbleOutputVisitor;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

/**
 * Created by szf on 2018/9/17.
 */
public final class SqlStringUtil {

    private SqlStringUtil() {

    }

    public static String toSQLString(SQLObject sqlObject) {
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = new DbleOutputVisitor(out);
        SQLUtils.FormatOption option = new SQLUtils.FormatOption(true, true);
        visitor.setUppCase(option.isUppCase());
        visitor.setPrettyFormat(option.isPrettyFormat());
        visitor.setParameterized(option.isParameterized());
        sqlObject.accept(visitor);
        String sql = out.toString();
        return sql;
    }

    public static String getSqlType(int sqlType) {
        String type;
        switch (sqlType) {
            case ServerParse.DDL:
            case ServerParse.CREATE_DATABASE:
            case ServerParse.CREATE_VIEW:
            case ServerParse.DROP_VIEW:
            case ServerParse.DROP_TABLE:
                type = "DDL";
                break;
            case ServerParse.INSERT:
                type = "Insert";
                break;
            case ServerParse.SELECT:
                type = "Select";
                break;
            case ServerParse.UPDATE:
                type = "Update";
                break;
            case ServerParse.DELETE:
                type = "Delete";
                break;
            case ServerParse.LOAD_DATA_INFILE_SQL:
                type = "Loaddata";
                break;
            case ServerParse.BEGIN:
                type = "Begin";
                break;
            case ServerParse.COMMIT:
                type = "Commit";
                break;
            case ServerParse.ROLLBACK:
                type = "Rollback";
                break;
            case ServerParse.SET:
                type = "Set";
                break;
            case ServerParse.SHOW:
                type = "Show";
                break;
            default:
                type = "Other";

        }
        return type;
    }
}
