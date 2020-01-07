/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.server.parser.DbleOutputVisitor;
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
}
