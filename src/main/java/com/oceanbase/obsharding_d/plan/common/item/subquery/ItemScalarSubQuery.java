/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.subquery;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class ItemScalarSubQuery extends ItemSingleRowSubQuery {
    public ItemScalarSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, query, false, metaManager, usrVariables, charsetIndex, hintPlanInfo);
        this.charsetIndex = charsetIndex;
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
        ItemScalarSubQuery cloneItem = new ItemScalarSubQuery(this.currentDb, this.query, this.metaManager, this.usrVariables, charsetIndex, this.hintPlanInfo);
        cloneItem.value = this.value;
        return cloneItem;
    }

}
