/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.node;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.NamedField;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemFuncGroupConcat;
import com.oceanbase.obsharding_d.plan.util.ToStringUtil;
import com.oceanbase.obsharding_d.route.parser.druid.RouteTableConfigInfo;

import java.util.ArrayList;
import java.util.List;

public class ModifyNode extends PlanNode {

    /**
     * filter in where
     */
    List<ItemFuncEqual> setItemList = new ArrayList<>();

    public PlanNodeType type() {
        return PlanNodeType.MODIFY;
    }

    public ModifyNode(PlanNode child) {
        this(child, null);
    }

    public ModifyNode(PlanNode child, Item filter) {
        this.whereFilter = filter;
        for (PlanNode childChild : child.getChildren()) {
            this.addChild(childChild);
        }
        this.keepFieldSchema = false;
    }

    @Override
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        if (columnsSelected.size() > index) {
            Item sourceColumns = columnsSelected.get(index);
            for (int i = 0; i < this.getChild().columnsSelected.size(); i++) {
                Item cSelected = this.getChild().columnsSelected.get(i);
                if (cSelected.getAlias() != null && cSelected.getAlias().equals(sourceColumns.getItemName())) {
                    return this.getChild().findFieldSourceFromIndex(i);
                } else if (cSelected.getAlias() == null && cSelected.getItemName().equals(sourceColumns.getItemName())) {
                    return this.getChild().findFieldSourceFromIndex(i);
                }
            }
            return null;
        }
        return null;
    }


    public void setChild(PlanNode child) {
        if (child == null) {
            return;
        }
        children.clear();
        addChild(child);
    }

    @Override
    public String getPureName() {
        return this.getChild().getAlias();
    }

    @Override
    public String getPureSchema() {
        return this.getChild().getPureSchema();
    }

    @Override
    public ModifyNode copy() {
        ModifyNode newTableNode = new ModifyNode(this.getChild().copy());
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    @Override
    public int getHeight() {
        return getChild().getHeight() + 1;
    }

    public List<ItemFuncEqual> getSetItemList() {
        return setItemList;
    }

    public void addSetItem(ItemFuncEqual setItem) {
        this.setItemList.add(setItem);
    }


    public PlanNode getLeftNode() {
        if (children.isEmpty())
            return null;
        return children.get(0);

    }

    public PlanNode getRightNode() {
        if (children.size() < 2)
            return null;
        return children.get(1);
    }

    public void setUpFields() {
        super.setUpFields();
        setUpSetItem();
    }

    private void setUpSetItem() {
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);
        for (ItemFuncEqual setItem : setItemList) {
            for (Item argument : setItem.arguments()) {
                setUpItem(argument);
                if (argument instanceof ItemFuncGroupConcat) {
                    ((ItemFuncGroupConcat) argument).fixOrders(nameContext, referContext);
                }
                NamedField field = makeOutNamedField(argument);
                if (outerFields.containsKey(field) && isDuplicateField(this))
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "duplicate field");
                outerFields.put(field, argument);
            }
            setItem.setItemName(null);
        }
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        String tabContent = ToStringUtil.getTab(level + 1);

        ToStringUtil.appendln(sb, tabTittle + "Update");
        ToStringUtil.appendln(sb, tabContent + "set: " + ToStringUtil.itemListString(setItemList));
        ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
        ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
        if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
            ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
            ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
        }
        ToStringUtil.appendln(sb, tabContent + "sql: " + this.getSql());

        ToStringUtil.appendln(sb, tabContent + "left:");
        sb.append(this.getLeftNode().toString(level + 2));
        ToStringUtil.appendln(sb, tabContent + "right:");
        sb.append(this.getRightNode().toString(level + 2));
        return sb.toString();
    }

}
