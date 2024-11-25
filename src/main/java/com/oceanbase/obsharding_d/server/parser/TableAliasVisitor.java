/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.parser;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.ArrayList;
import java.util.List;

public class TableAliasVisitor extends MySqlASTVisitorAdapter {

    private List<String> tableAlias = new ArrayList<>(5);

    public boolean visit(SQLExprTableSource x) {
        String alias = x.getAlias();
        tableAlias.add(alias);
        return true;
    }

    public List<String> getAlias() {
        return tableAlias;
    }

}
