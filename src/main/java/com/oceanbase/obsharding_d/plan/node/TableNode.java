/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.node;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.HintNestLoopHelper;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.GlobalTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.SingleTableConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.plan.NamedField;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemBasicConstant;
import com.oceanbase.obsharding_d.plan.common.item.ItemField;
import com.oceanbase.obsharding_d.plan.util.ToStringUtil;
import com.oceanbase.obsharding_d.route.parser.druid.RouteTableConfigInfo;
import com.alibaba.druid.sql.ast.SQLHint;

import java.sql.SQLNonTransientException;
import java.util.*;

public class TableNode extends PlanNode {

    private String schema;
    private String tableName;
    private TableMeta tableMeta;
    private List<String> columns;
    private List<SQLHint> hintList;
    private int charsetIndex = 63;
    private HintNestLoopHelper hintNestLoopHelper;

    private TableNode() {
    }

    public void setHintNestLoopHelper(HintNestLoopHelper hintNestLoopHelper) {
        this.hintNestLoopHelper = hintNestLoopHelper;
    }

    public HintNestLoopHelper getHintNestLoopHelper() {
        return hintNestLoopHelper;
    }

    public TableNode(String schema, String viewName, List<String> columns) {
        if (schema == null || viewName == null)
            throw new RuntimeException("Schema name or Table name is null!");
        this.schema = schema;
        this.tableName = viewName;
        ServerConfig config = OBsharding_DServer.getInstance().getConfig();
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            this.schema = this.schema.toLowerCase();
            this.tableName = this.tableName.toLowerCase();
        }
        SchemaConfig schemaConfig = config.getSchemas().get(this.schema);
        if (schemaConfig == null) {
            throw new RuntimeException("schema " + this.schema + " doesn't exist!");
        }

        this.columns = columns;
        this.setNoshardNode(new HashSet<>(Collections.singletonList(schemaConfig.getDefaultSingleNode())));
        this.referedTableNodes.add(this);
        this.keepFieldSchema = true;
    }

    public TableNode(String catalog, String tableName, ProxyMetaManager metaManager, int charsetIndex) throws SQLNonTransientException {
        if (catalog == null || tableName == null)
            throw new RuntimeException("Table db or name is null error!");
        this.charsetIndex = charsetIndex;
        this.schema = catalog;
        this.tableName = tableName;
        ServerConfig config = OBsharding_DServer.getInstance().getConfig();
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            this.schema = this.schema.toLowerCase();
            this.tableName = this.tableName.toLowerCase();
        }
        SchemaConfig schemaConfig = config.getSchemas().get(this.schema);
        if (schemaConfig == null) {
            throw new RuntimeException("schema " + this.schema + " doesn't exist!");
        }
        this.tableMeta = metaManager.getSyncTableMeta(this.schema, this.tableName);
        BaseTableConfig tableConfig = schemaConfig.getTables().get(this.tableName);
        if (this.tableMeta == null) {
            String errorMsg = "table " + this.tableName + " doesn't exist!";
            if (tableConfig != null || schemaConfig.getDefaultShardingNodes() != null) {
                errorMsg += "You should create it OR reload metadata";
            }
            throw new RuntimeException(errorMsg);
        }
        this.referedTableNodes.add(this);
        if (tableConfig == null) {
            this.setNoshardNode(new HashSet<>(Collections.singletonList(schemaConfig.getDefaultSingleNode())));
        } else {
            if (!(tableConfig instanceof GlobalTableConfig) && !(tableConfig instanceof SingleTableConfig)) {
                this.unGlobalTableCount = 1;
            }
            this.setNoshardNode(new HashSet<>(tableConfig.getShardingNodes()));

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
            for (ColumnMeta cm : tableMeta.getColumns()) {
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
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        if (columnsSelected.get(index) instanceof ItemBasicConstant) {
            return new RouteTableConfigInfo(schema, null, null, columnsSelected.get(index));
        } else {
            BaseTableConfig info = OBsharding_DServer.getInstance().getConfig().getSchemas().get(schema).getTables().get(tableName);
            if (info instanceof ShardingTableConfig) {
                ShardingTableConfig shardingTableConfig = (ShardingTableConfig) info;
                if (shardingTableConfig.getShardingColumn().equalsIgnoreCase(columnsSelected.get(index).getItemName())) {
                    return new RouteTableConfigInfo(schema, OBsharding_DServer.getInstance().getConfig().
                            getSchemas().get(schema).getTables().get(tableName), alias, null);
                }
            } else {
                return null;
            }
        }
        return null;
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
        newTableNode.tableMeta = this.tableMeta;
        newTableNode.columns = this.columns;
        newTableNode.referedTableNodes.add(newTableNode);
        if (this.getNoshardNode() != null) {
            Set<String> noShardNode = new HashSet<>(this.getNoshardNode());
            newTableNode.setNoshardNode(noShardNode);
        }

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
