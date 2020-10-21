/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemResultField;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.util.Map;

public abstract class ItemSubQuery extends ItemResultField {
    protected SQLSelectQuery query;
    protected String currentDb;
    protected PlanNode planNode;
    protected ProxyMetaManager metaManager;
    protected Map<String, String> usrVariables;

    public enum SubSelectType {
        UNKNOWN_SUBS, SINGLEROW_SUBS, EXISTS_SUBS, IN_SUBS, ALL_SUBS, ANY_SUBS
    }

    public SubSelectType subType() {
        return SubSelectType.UNKNOWN_SUBS;
    }

    public ItemSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex) {
        this.charsetIndex = charsetIndex;
        this.query = query;
        this.currentDb = currentDb;
        this.metaManager = metaManager;
        this.usrVariables = usrVariables;
        init();
    }

    @Override
    public ItemType type() {
        return ItemType.SUBSELECT_ITEM;
    }

    private void init() {
        MySQLPlanNodeVisitor pv = new MySQLPlanNodeVisitor(currentDb, charsetIndex, metaManager, true, usrVariables);
        pv.visit(this.query);
        this.planNode = pv.getTableNode();
        this.withSubQuery = true;
        PlanNode test = this.planNode.copy();
        try {
            test.setUpFields();
            planNode.setUpFields();
        } catch (Exception e) {
            this.correlatedSubQuery = true;
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
        return false;
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

}
