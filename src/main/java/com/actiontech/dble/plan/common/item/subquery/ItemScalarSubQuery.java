/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.util.List;

public class ItemScalarSubQuery extends ItemSingleRowSubQuery {

    protected boolean orderCondition;

    public ItemScalarSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager) {
        super(currentDb, query, false, metaManager);
        if (this.planNode.getColumnsSelected().size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ER_OPERAND_COLUMNS, "", "Operand should contain 1 column(s)");
        }
        if (!this.correlatedSubQuery) {
            if ((this.planNode.getLimitFrom() == -1)) {
                this.planNode.setLimitFrom(0);
                this.planNode.setLimitTo(2);
            } else if (this.planNode.getLimitTo() > 2) {
                this.planNode.setLimitTo(2);
            }
        }
    }

    @Override
    public SubSelectType subType() {
        return SubSelectType.SINGLEROW_SUBS;
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public SQLExpr toExpression() {
        SQLSelect sqlSelect = new SQLSelect(query);
        return new SQLQueryExpr(sqlSelect);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fieldList) {
        return new ItemScalarSubQuery(this.currentDb, this.query, this.metaManager);
    }

    public boolean isOrderCondition() {
        return orderCondition;
    }

    public void setOrderCondition(boolean orderCondition) {
        this.orderCondition = orderCondition;
    }

}
