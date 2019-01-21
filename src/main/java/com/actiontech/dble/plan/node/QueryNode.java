/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.util.ToStringUtil;

/*
 * query a logic table ,in fact, it is derived sub query
 */
public class QueryNode extends PlanNode {

    public PlanNodeType type() {
        return PlanNodeType.QUERY;
    }

    public QueryNode(PlanNode child) {
        this(child, null);
    }

    public QueryNode(PlanNode child, Item filter) {
        this.whereFilter = filter;
        this.setChild(child);
        if (child != null) {
            child.setWithSubQuery(true); // the default is subQuery
        }
        this.keepFieldSchema = false;
    }

    public void setChild(PlanNode child) {
        if (child == null) {
            return;
        }
        children.clear();
        addChild(child);
    }
    @Override
    protected void setUpInnerFields() {
        innerFields.clear();
        for (PlanNode child : children) {
            child.setUpFields();
            for (NamedField childOutField : child.outerFields.keySet()) {
                NamedField tmpField = new NamedField(null, this.getAlias(), childOutField.getName(), childOutField.planNode);
                if (innerFields.containsKey(tmpField) && getParent() != null)
                    throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "42S21", "Duplicate column name '" + childOutField.getName() + "'");
                innerFields.put(tmpField, childOutField);
            }
        }
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
    public QueryNode copy() {
        QueryNode newTableNode = new QueryNode(this.getChild().copy());
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    @Override
    public int getHeight() {
        return getChild().getHeight() + 1;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        String tabContent = ToStringUtil.getTab(level + 1);
        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + "SubQuery" + " as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + "SubQuery");
        }
        ToStringUtil.appendln(sb, tabContent + "isDistinct: " + isDistinct());
        ToStringUtil.appendln(sb, tabContent + "columns: " + ToStringUtil.itemListString(columnsSelected));
        ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
        ToStringUtil.appendln(sb, tabContent + "having: " + ToStringUtil.itemString(havingFilter));
        ToStringUtil.appendln(sb, tabContent + "groupBy: " + ToStringUtil.orderListString(groups));
        ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
        if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
            ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
            ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
        }
        ToStringUtil.appendln(sb, tabContent + "sql: " + this.getSql());
        if (this.getChild() != null) {
            ToStringUtil.appendln(sb, tabContent + "from:");
            sb.append(this.getChild().toString(level + 2));
        }
        return sb.toString();
    }

}
