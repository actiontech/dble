/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/*
 * ALL/ANY/SOME sub Query
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.util.List;
import java.util.Map;

public class ItemAllAnySubQuery extends ItemMultiRowSubQuery {
    private boolean isAll;
    private SQLBinaryOperator operator;
    public ItemAllAnySubQuery(String currentDb, SQLSelectQuery query, SQLBinaryOperator operator, boolean isAll, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex);
        this.isAll = isAll;
        this.operator = operator;
        if (this.planNode.getColumnsSelected().size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ER_OPERAND_COLUMNS, "", "Operand should contain 1 column(s)");
        }
        this.select = this.planNode.getColumnsSelected().get(0);
    }

    @Override
    public SubSelectType subType() {
        return isAll ? SubSelectType.ALL_SUBS : SubSelectType.ANY_SUBS;
    }


    @Override
    public SQLExpr toExpression() {
        SQLSelect sqlSelect = new SQLSelect(query);
        if (isAll) {
            return new SQLAllExpr(sqlSelect);
        } else {
            return new SQLAnyExpr(sqlSelect);
        }
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemAllAnySubQuery(this.currentDb, this.query, this.operator, this.isAll, this.metaManager, this.usrVariables, this.charsetIndex);
    }

    public boolean isAll() {
        return isAll;
    }

    public SQLBinaryOperator getOperator() {
        return operator;
    }
}
