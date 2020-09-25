/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.util.ToStringUtil;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

import static com.actiontech.dble.plan.node.PlanNode.PlanNodeType.MANAGER_TABLE;

public class ManagerTableNode extends PlanNode {
    private String schema;
    private String tableName;
    private boolean needSendMaker;
    private ManagerBaseTable table;

    private ManagerTableNode() {
    }

    public ManagerTableNode(String catalog, String tableName) throws SQLNonTransientException {
        if (catalog == null || tableName == null)
            throw new RuntimeException("Schema name or Table name is null!");
        this.schema = catalog.toLowerCase();
        this.tableName = tableName.toLowerCase();

        if (!schema.equals(ManagerSchemaInfo.SCHEMA_NAME)) {
            throw new RuntimeException("schema " + this.schema + " doesn't exist!");
        }
        ManagerBaseTable managerTable = ManagerSchemaInfo.getInstance().getTables().get(this.tableName);
        if (managerTable == null) {
            throw new RuntimeException("table " + this.tableName + " doesn't exist!");
        } else {
            this.table = managerTable;
        }
        this.keepFieldSchema = true;
    }

    @Override
    protected void setUpInnerFields() {
        innerFields.clear();
        String tmpTable = alias == null ? tableName : alias;
        for (ColumnMeta cm : table.getColumnsMeta()) {
            NamedField tmpField = new NamedField(schema, tmpTable, cm.getName(), this);
            innerFields.put(tmpField, tmpField);
        }

    }

    @Override
    protected void dealStarColumn() {
        List<Item> newSelects = new ArrayList<>();
        for (Item sel : columnsSelected) {
            if (!sel.isWild())
                newSelects.add(sel);
            else {
                for (NamedField innerField : innerFields.keySet()) {
                    ItemField col = new ItemField(null, sel.getTableName(), innerField.getName());
                    newSelects.add(col);
                }
            }
        }
        columnsSelected = newSelects;
    }

    @Override
    public PlanNodeType type() {
        return MANAGER_TABLE;
    }

    @Override
    public String getPureName() {
        return this.getTableName();
    }

    @Override
    public String getPureSchema() {
        return this.getSchema();
    }

    @Override
    public int getHeight() {
        return 1;
    }

    @Override
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        return null;
    }

    @Override
    public ManagerTableNode copy() {
        ManagerTableNode newTableNode = new ManagerTableNode();
        newTableNode.schema = this.schema;
        newTableNode.tableName = this.tableName;
        newTableNode.table = this.table;
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        String tabContent = ToStringUtil.getTab(level + 1);
        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + "Query from " + this.getTableName() + " as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + "Query from " + this.getTableName());
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
        return sb.toString();
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }


    public boolean isNeedSendMaker() {
        return needSendMaker;
    }

    public void setNeedSendMaker(boolean needSendMaker) {
        this.needSendMaker = needSendMaker;
    }

}
