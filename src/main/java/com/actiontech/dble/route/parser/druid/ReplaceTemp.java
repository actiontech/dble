/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2017/8/22.
 */
public class ReplaceTemp extends MySqlReplaceStatement {

    @Override
    public List<SQLInsertStatement.ValuesClause> getValuesList() {
        return valuesList;
    }

    public void setValuesList(List<SQLInsertStatement.ValuesClause> valuesList) {
        this.valuesList = valuesList;
    }

    private List<SQLInsertStatement.ValuesClause> valuesList = new ArrayList();


    @Override
    public List<SQLExpr> getColumns() {
        return columns;
    }

    public void setColumns(List<SQLExpr> columns) {
        this.columns = columns;
    }

    private List<SQLExpr> columns = new ArrayList();

    public ReplaceTemp(MySqlReplaceStatement statement) {
        this.setLowPriority(statement.isLowPriority());
        this.setDelayed(statement.isDelayed());
        this.setValuesList(statement.getValuesList());
        this.setTableSource(statement.getTableSource());
        this.setColumns(statement.getColumns());
        this.setDbType(statement.getDbType());
        this.setParent(statement.getParent());
        this.attributes = statement.getAttributes();
    }

    public String toString() {
        return super.toString();
    }

}
