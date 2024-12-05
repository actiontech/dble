/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.subquery;

import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.context.NameResolutionContext;
import com.oceanbase.obsharding_d.plan.common.context.ReferContext;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemResultField;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.oceanbase.obsharding_d.plan.visitor.MySQLPlanNodeVisitor;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.util.Map;

public abstract class ItemSubQuery extends ItemResultField {
    protected SQLSelectQuery query;
    protected String currentDb;
    protected PlanNode planNode;
    protected ProxyMetaManager metaManager;
    protected Map<String, String> usrVariables;
    protected HintPlanInfo hintPlanInfo;

    public enum SubSelectType {
        UNKNOWN_SUBS, SINGLEROW_SUBS, EXISTS_SUBS, IN_SUBS, ALL_SUBS, ANY_SUBS
    }

    public SubSelectType subType() {
        return SubSelectType.UNKNOWN_SUBS;
    }

    public ItemSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        this.charsetIndex = charsetIndex;
        this.query = query;
        this.currentDb = currentDb;
        this.metaManager = metaManager;
        this.usrVariables = usrVariables;
        this.hintPlanInfo = hintPlanInfo;
        if (query != null) {
            init();
        }
    }

    @Override
    public ItemType type() {
        return ItemType.SUBSELECT_ITEM;
    }

    private void init() {
        MySQLPlanNodeVisitor pv = new MySQLPlanNodeVisitor(currentDb, charsetIndex, metaManager, true, usrVariables, hintPlanInfo);
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
