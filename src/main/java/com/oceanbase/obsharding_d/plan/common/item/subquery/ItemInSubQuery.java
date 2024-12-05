/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.subquery;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.context.NameResolutionContext;
import com.oceanbase.obsharding_d.plan.common.context.ReferContext;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class ItemInSubQuery extends ItemMultiRowSubQuery {
    private final boolean isNeg;
    protected Item leftOperand;

    public ItemInSubQuery(String currentDb, SQLSelectQuery query, Item leftOperand, boolean isNeg, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex, hintPlanInfo);
        this.leftOperand = leftOperand;
        this.isNeg = isNeg;
        this.charsetIndex = charsetIndex;
        if (this.planNode.getColumnsSelected().size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ER_OPERAND_COLUMNS, "", "Operand should contain 1 column(s)");
        }
        this.select = this.planNode.getColumnsSelected().get(0);
    }

    public Item fixFields(NameResolutionContext context) {
        super.fixFields(context);
        leftOperand = leftOperand.fixFields(context);
        getReferTables().addAll(leftOperand.getReferTables());
        return this;
    }

    /**
     * added to construct all refers in an item
     *
     * @param context
     */
    public void fixRefer(ReferContext context) {
        super.fixRefer(context);
        leftOperand.fixRefer(context);
    }

    @Override
    public SubSelectType subType() {
        return SubSelectType.IN_SUBS;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr expr = leftOperand.toExpression();
        SQLSelect sqlSelect = new SQLSelect(query);
        SQLInSubQueryExpr inSub = new SQLInSubQueryExpr(sqlSelect);
        inSub.setExpr(expr);
        inSub.setNot(isNeg);
        return inSub;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        ItemInSubQuery cloneItem = new ItemInSubQuery(this.currentDb, this.query, this.leftOperand.cloneStruct(), this.isNeg, this.metaManager, this.usrVariables, this.charsetIndex, this.hintPlanInfo);
        cloneItem.value = this.value;
        return cloneItem;
    }

    public Item getLeftOperand() {
        return leftOperand;
    }

    public boolean isNeg() {
        return isNeg;
    }
}
