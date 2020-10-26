/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.TableConfig.TableTypeEnum;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.util.ToStringUtil;
import com.alibaba.druid.sql.ast.SQLHint;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class TableNode extends PlanNode {

    private String schema;
    private String tableName;
    private StructureMeta.TableMeta tableMeta;
    private List<String> columns;
    private List<SQLHint> hintList;
    private int charsetIndex;

    private TableNode() {
    }

    public TableNode(String schema, String viewName, List<String> columns) {
        if (schema == null || viewName == null)
            throw new RuntimeException("Table db or name is null error!");
        this.schema = schema;
        this.tableName = viewName;
        ServerConfig config = DbleServer.getInstance().getConfig();
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            this.schema = this.schema.toLowerCase();
            this.tableName = this.tableName.toLowerCase();
        }
        SchemaConfig schemaConfig = config.getSchemas().get(this.schema);
        if (schemaConfig == null) {
            throw new RuntimeException("schema " + this.schema + " doesn't exist!");
        }

        this.columns = columns;
        this.setNoshardNode(new HashSet<>(Collections.singletonList(schemaConfig.getDataNode())));
        this.referedTableNodes.add(this);
        this.keepFieldSchema = true;
    }

    public TableNode(String catalog, String tableName, ProxyMetaManager metaManager, int charsetIndex) throws SQLNonTransientException {
        if (catalog == null || tableName == null)
            throw new RuntimeException("Table db or name is null error!");
        this.charsetIndex = charsetIndex;
        this.schema = catalog;
        this.tableName = tableName;
        ServerConfig config = DbleServer.getInstance().getConfig();
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            this.schema = this.schema.toLowerCase();
            this.tableName = this.tableName.toLowerCase();
        }
        SchemaConfig schemaConfig = config.getSchemas().get(this.schema);
        if (schemaConfig == null) {
            throw new RuntimeException("schema " + this.schema + " doesn't exist!");
        }
        this.tableMeta = metaManager.getSyncTableMeta(this.schema, this.tableName);
        TableConfig tableConfig = schemaConfig.getTables().get(this.tableName);
        if (this.tableMeta == null) {
            String errorMsg = "table " + this.tableName + " doesn't exist!";
            if (tableConfig != null || schemaConfig.getDataNode() != null) {
                errorMsg += "You should create it OR reload metadata";
            }
            throw new RuntimeException(errorMsg);
        }
        this.referedTableNodes.add(this);
        if (tableConfig == null) {
            this.setNoshardNode(new HashSet<>(Collections.singletonList(schemaConfig.getDataNode())));
        } else {
            if (tableConfig.getTableType() != TableTypeEnum.TYPE_GLOBAL_TABLE && !tableConfig.isNoSharding()) {
                this.unGlobalTableCount = 1;
            }
            this.setNoshardNode(new HashSet<>(tableConfig.getDataNodes()));
        }
        this.keepFieldSchema = true;
    }

    public PlanNodeType type() {
        return PlanNodeType.TABLE;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    protected void setUpInnerFields() {
        innerFields.clear();
        String tmpTable = alias == null ? tableName : alias;
        if (tableMeta != null) {
            for (StructureMeta.ColumnMeta cm : tableMeta.getColumnsList()) {
                NamedField tmpField = new NamedField(schema, tmpTable, cm.getName(), this);
                tmpField.setCharsetIndex(charsetIndex);
                innerFields.put(tmpField, tmpField);
            }
        } else {
            for (String col : columns) {
                NamedField tmpField = new NamedField(schema, tmpTable, col, this);
                tmpField.setCharsetIndex(charsetIndex);
                innerFields.put(tmpField, tmpField);
            }
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
                    ItemField col = new ItemField(null, sel.getTableName(), innerField.getName(), charsetIndex);
                    newSelects.add(col);
                }
            }
        }
        columnsSelected = newSelects;
    }

    public TableNode copy() {
        TableNode newTableNode = new TableNode();
        newTableNode.schema = this.schema;
        newTableNode.tableName = this.tableName;
        newTableNode.tableMeta = this.tableMeta == null ? null : this.tableMeta.toBuilder().build();
        newTableNode.columns = this.columns;
        newTableNode.charsetIndex = this.charsetIndex;
        newTableNode.referedTableNodes.add(newTableNode);
        newTableNode.setNoshardNode(this.getNoshardNode());

        this.copySelfTo(newTableNode);
        newTableNode.setHintList(this.hintList);
        return newTableNode;
    }

    @Override
    public String getPureName() {
        return this.getTableName();
    }

    @Override
    public String getPureSchema() {
        return this.getSchema();
    }

    public String getSchema() {
        return this.schema;
    }

    @Override
    public int getHeight() {
        return 1;
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

    public List<SQLHint> getHintList() {
        return hintList;
    }

    public void setHintList(List<SQLHint> hintList) {
        this.hintList = hintList;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }
}
