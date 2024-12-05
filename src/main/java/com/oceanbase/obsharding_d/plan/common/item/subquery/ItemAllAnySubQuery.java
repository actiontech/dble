/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/*
 * ALL/ANY/SOME sub Query
 */
package com.oceanbase.obsharding_d.plan.common.item.subquery;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class ItemAllAnySubQuery extends ItemMultiRowSubQuery {
    private boolean isAll;
    private SQLBinaryOperator operator;

    public ItemAllAnySubQuery(String currentDb, SQLSelectQuery query, SQLBinaryOperator operator, boolean isAll, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex, hintPlanInfo);
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
        ItemAllAnySubQuery cloneItem = new ItemAllAnySubQuery(this.currentDb, this.query, this.operator, this.isAll, this.metaManager, this.usrVariables, this.charsetIndex, this.hintPlanInfo);
        cloneItem.value = this.value;
        return cloneItem;
    }

    public boolean isAll() {
        return isAll;
    }

    public SQLBinaryOperator getOperator() {
        return operator;
    }
}
