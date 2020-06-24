/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.util.ToStringUtil;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;

import java.util.*;

/**
 * @author ActionTech
 */
public class MergeNode extends PlanNode {

    private List<Item> comeInFields;

    public PlanNodeType type() {
        return PlanNodeType.MERGE;
    }

    private boolean union = false;

    public MergeNode() {
    }

    public MergeNode merge(PlanNode o) {
        this.addChild(o);
        return this;
    }

    // map for union filed name-> index
    public Map<String, Integer> getColIndexs() {
        Map<String, Integer> colIndexs = new HashMap<>();
        for (int index = 0; index < getColumnsSelected().size(); index++) {
            String name = getColumnsSelected().get(index).getItemName();
            colIndexs.put(name, index);
        }
        return colIndexs;
    }

    public String getPureName() {
        return null;
    }

    @Override
    public String getPureSchema() {
        return null;
    }

    public boolean isUnion() {
        return union;
    }

    public void setUnion(boolean union) {
        this.union = union;
    }

    public List<Item> getComeInFields() {
        // push down the union's ordern by
        //  union's select column may be different from  child's select
        // union handler's use the first child as oolumns,union sql's result is node.getcolumnSelected
        if (comeInFields == null)
            return getColumnsSelected();
        else
            return comeInFields;
    }

    @Override
    protected void setUpInnerFields() {
        innerFields.clear();
        boolean isFirst = true;
        int columnSize = 0;
        for (PlanNode child : children) {
            child.setUpFields();
            int childSelects = child.getColumnsSelected().size();
            if (isFirst) {
                columnSize = childSelects;
                isFirst = false;
            } else {
                if (columnSize != childSelects) {
                    throw new MySQLOutPutException(ErrorCode.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
                            "The used SELECT statements have a different number of columns");
                }
            }
        }
    }

    @Override
    protected void setUpSelects() {
        columnsSelected.clear();
        PlanNode firstNode = getChild();
        outerFields.clear();
        Set<NamedField> checkDup = new HashSet<>(firstNode.getOuterFields().size(), 1);
        for (NamedField coutField : firstNode.getOuterFields().keySet()) {
            NamedField testDupField = new NamedField(null, null, coutField.getName(), this);
            if (checkDup.contains(testDupField) && isDuplicateField(this)) {
                throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "", "Duplicate column name " + coutField.getName());
            }
            checkDup.add(testDupField);
            ItemField column = new ItemField(null, coutField.getTable(), coutField.getName());
            column.getReferTables().clear();
            column.getReferTables().add(coutField.planNode);
            NamedField tmpField = new NamedField(null, coutField.getTable(), coutField.getName(), this);
            outerFields.put(tmpField, column);
            getColumnsSelected().add(column);
        }
    }

    @Override
    public MergeNode copy() {
        MergeNode newMergeNode = new MergeNode();
        this.copySelfTo(newMergeNode);
        newMergeNode.setUnion(union);
        for (PlanNode child : children) {
            newMergeNode.addChild(child.copy());
        }
        return newMergeNode;
    }

    @Override
    public int getHeight() {
        int maxChildHeight = 0;
        for (PlanNode child : children) {
            int childHeight = child.getHeight();
            if (childHeight > maxChildHeight)
                maxChildHeight = childHeight;
        }
        return maxChildHeight + 1;
    }

    @Override
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        return null;
    }

    public void setComeInFields(List<Item> comeInFields) {
        this.comeInFields = comeInFields;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        String tabContent = ToStringUtil.getTab(level + 1);
        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Union all") + " as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Union all"));
        }
        ToStringUtil.appendln(sb, tabContent + "columns: " + ToStringUtil.itemListString(columnsSelected));
        ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
        ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
        if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
            ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
            ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
        }

        for (PlanNode node : children) {
            sb.append(node.toString(level + 2));
        }

        return sb.toString();
    }

}
