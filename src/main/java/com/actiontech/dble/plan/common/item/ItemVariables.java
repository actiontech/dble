/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.node.PlanNode;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemVariables extends Item {
    private String name;
    private Item ref;
    public ItemVariables(String name, Item ref) {
        this.name = name;
        this.ref = ref;
    }

    @Override
    public Item fixFields(NameResolutionContext context) {
        getReferTables().clear();
        this.getReferTables().add(context.getPlanNode());
        return this;
    }

    @Override
    public final void fixRefer(ReferContext context) {
        PlanNode node = context.getPlanNode();
        PlanNode tn = getReferTables().iterator().next();
        node.addSelToReferedMap(tn, this);
    }
    @Override
    public ItemType type() {
        return ItemType.VARIABLE_ITEM;
    }

    @Override
    public BigDecimal valReal() {
        return ref.valReal();
    }

    @Override
    public BigInteger valInt() {
        return ref.valInt();
    }

    @Override
    public String valStr() {
        return ref.valStr();
    }

    @Override
    public BigDecimal valDecimal() {
        return ref.valDecimal();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return ref.getDate(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return ref.getTime(ltime);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLVariantRefExpr(name);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemVariables(name, ref.cloneItem());
    }
    public Item getRef() {
        return ref;
    }

}
