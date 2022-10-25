/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class UpdateItemSubQuery extends ItemMultiColumnARowSubQuery {
    private final boolean isNeg;
    protected Item leftOperand;
    private PlanNode queryNode;

    public UpdateItemSubQuery(String currentDb, SQLSelectQuery query, Item leftOperand, boolean isNeg, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex, hintPlanInfo);
        this.leftOperand = leftOperand;
        this.isNeg = isNeg;
        this.charsetIndex = charsetIndex;
        this.select = this.planNode.getColumnsSelected();
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
        UpdateItemSubQuery cloneItem = new UpdateItemSubQuery(this.currentDb, this.query, this.leftOperand.cloneStruct(), this.isNeg, this.metaManager, this.usrVariables, this.charsetIndex, this.hintPlanInfo);
        cloneItem.value = this.value;
        return cloneItem;
    }

    public PlanNode getQueryNode() {
        return queryNode;
    }

    public void setQueryNode(PlanNode queryNode) {
        this.queryNode = queryNode;
    }

    public boolean isNeg() {
        return isNeg;
    }
}

