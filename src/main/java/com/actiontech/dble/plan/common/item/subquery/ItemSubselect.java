/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemResultField;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

public abstract class ItemSubselect extends ItemResultField {
    protected SQLSelectQuery query;
    private String currentDb;
    private PlanNode planNode;

    public enum SubSelectType {
        UNKNOWN_SUBS, SINGLEROW_SUBS, EXISTS_SUBS, IN_SUBS, ALL_SUBS, ANY_SUBS
    }

    public SubSelectType substype() {
        return SubSelectType.UNKNOWN_SUBS;
    }

    public ItemSubselect(String currentDb, SQLSelectQuery query) {
        this.query = query;
        this.currentDb = currentDb;
        init();
    }

    @Override
    public ItemType type() {
        return ItemType.SUBSELECT_ITEM;
    }

    private void init() {
        MySQLPlanNodeVisitor pv = new MySQLPlanNodeVisitor(currentDb, charsetIndex);
        pv.visit(this.query);
        this.planNode = pv.getTableNode();
        if (planNode.type() != PlanNodeType.NONAME) {
            this.withSubQuery = true;
        }
    }

    public void reset() {
        this.nullValue = true;
    }

    @Override
    public final boolean isNull() {
        updateNullValue();
        return nullValue;
    }

    @Override
    public boolean fixFields() {
        throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "not supported!");
    }

    public Item fixFields(NameResolutionContext context) {
        this.planNode.setUpFields();
        return this;
    }

    /**
     * added to construct all refers in an item
     *
     * @param context
     */
    public void fixRefer(ReferContext context) {
        if (context.isPushDownNode())
            return;
        else
            context.getPlanNode().getSubSelects().add(this);
    }

    @Override
    public String funcName() {
        return "subselect";
    }

    public boolean execute() {
        // TODO
        return false;
    }

    public PlanNode getPlanNode() {
        return planNode;
    }

    public void setPlanNode(PlanNode planNode) {
        this.planNode = planNode;
    }

    @Override
    public String toString() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported!");
    }
}
