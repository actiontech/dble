/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by szf on 2017/9/29.
 */
public class ViewMeta {
    private String viewName;
    private String createSql;
    private String selectSql;
    private PlanNode viewQuery;

    private List<String> viewColumnMeta;
    private String schema;
    private ProxyMetaManager tmManager;
    private long timestamp;

    public ViewMeta(String schema, String createSql, ProxyMetaManager tmManager) {
        this.createSql = createSql;
        this.schema = schema;
        this.tmManager = tmManager;
    }

    public void init() throws Exception {
        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        viewParser.parseCreateView(this);
        //check if the select part has
        checkDuplicate(viewParser.getType());
        parseSelectInView();
        if (viewParser.getType() != ViewMetaParser.TYPE_CREATE_VIEW) {
            createSql = "create or replace view " + viewName + " as " + selectSql;
        }
    }

    private void parseSelectInView() throws SQLException {
        SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);
        MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(this.schema, 63, tmManager, false, null);
        msv.visit(selectStatement.getSelect().getQuery());
        PlanNode selNode = msv.getTableNode();

        boolean noShardingView = true;
        if (selNode.getReferedTableNodes().size() == 0) {
            noShardingView = false;
        }
        for (TableNode tableNode : selNode.getReferedTableNodes()) {
            if (DbleServer.getInstance().getConfig().getSchemas().get(tableNode.getSchema()).isNoSharding()) {
                if (!schema.equals(tableNode.getSchema())) {
                    noShardingView = false;
                    break;
                }
            } else {
                noShardingView = false;
                break;
            }
        }

        if (noShardingView) {
            if (viewColumnMeta == null) {
                selNode.setUpFields();
                List<Item> selectItems = selNode.getColumnsSelected();
                viewColumnMeta = new ArrayList<>(selectItems.size());
                for (Item item : selectItems) {
                    String alias = item.getAlias() == null ? item.getItemName() : item.getAlias();
                    viewColumnMeta.add(StringUtil.removeBackQuote(alias));
                }
            }
            viewQuery = new TableNode(schema, viewName, viewColumnMeta);
        } else {
            if (selNode instanceof MergeNode) {
                selNode.setUpFields();
                this.setFieldsAlias(selNode, true);
            } else {
                selNode.setUpFields();
                this.setFieldsAlias(selNode, false);
            }
            viewQuery = new QueryNode(selNode);
        }
    }

    public void addMeta(boolean isNeedPersistence) throws SQLNonTransientException {
        try {
            tmManager.addMetaLock(schema, viewName, createSql);
            if (isNeedPersistence) {
                ProxyMeta.getInstance().getTmManager().getRepository().put(schema, viewName, this.createSql);
            }
            tmManager.getCatalogs().get(schema).getViewMetas().put(viewName, this);
        } finally {
            tmManager.removeMetaLock(schema, viewName);
        }
    }

    private void checkDuplicate(int type) throws SQLException {

        ViewMeta viewNode = tmManager.getCatalogs().get(schema).getViewMetas().get(viewName);
        //.getSyncView(schema,viewName);
        TableMeta tableMeta = tmManager.getCatalogs().get(schema).getTableMeta(viewName);
        //if the alter table
        if (type == ViewMetaParser.TYPE_ALTER_VIEW) {
            if (viewNode == null) {
                throw new SQLException("Table '" + viewName + "' doesn't exist", "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
        }

        if (viewColumnMeta != null) {
            Set<String> tempMap = new HashSet<String>();
            for (String t : viewColumnMeta) {
                if (tempMap.contains(t.trim())) {
                    throw new SQLException("Duplicate column name '" + t + "'", "HY000", ErrorCode.ER_WRONG_COLUMN_NAME);
                }
                tempMap.add(t.trim());
            }
        }

        // if the table with same name exists
        if (tableMeta != null) {
            throw new SQLException("Table '" + viewName + "' already exists", "42S01", ErrorCode.ER_TABLE_EXISTS_ERROR);
        }

        if (type == ViewMetaParser.TYPE_CREATE_VIEW) {
            // if the sql without replace & the view exists
            if (viewNode != null) {
                // return error because the view is exists
                throw new SQLException("Table '" + viewName + "' already exists", "42S01", ErrorCode.ER_TABLE_EXISTS_ERROR);
            }
        }
    }

    private void setFieldsAlias(PlanNode selNode, boolean isMergeNode) throws SQLException {
        if (viewColumnMeta == null) {
            List<Item> selectList = selNode.getColumnsSelected();
            Set<String> tempMap = new HashSet<>();
            for (Item t : selectList) {
                if (t.getAlias() != null) {
                    if (tempMap.contains(t.getAlias())) {
                        throw new SQLException("Duplicate column name '" + t + "'", "HY000", ErrorCode.ER_WRONG_COLUMN_NAME);
                    }
                    tempMap.add(t.getAlias());
                } else {
                    if (tempMap.contains(t.getItemName())) {
                        throw new SQLException("Duplicate column name '" + t + "'", "HY000", ErrorCode.ER_WRONG_COLUMN_NAME);
                    }
                    tempMap.add(t.getItemName());
                }
            }
            return;
        }

        List<Item> columnsSelected;
        if (isMergeNode) {
            PlanNode node = getFirstNoMergeNode(selNode);
            columnsSelected = node.getColumnsSelected();
        } else {
            columnsSelected = selNode.getColumnsSelected();
        }

        int size = columnsSelected.size();
        //check if the column number of view is same as the selectList in selectStatement
        if (viewColumnMeta.size() != size) {
            //return error
            throw new SQLException("The Column_list Size and Select_statement Size Not Match", "HY000", ErrorCode.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT);
        }
        Item column;
        for (int i = 0; i < size; i++) {
            column = columnsSelected.get(i);
            if (!column.getItemName().equalsIgnoreCase(viewColumnMeta.get(i).trim())) {
                column.setAlias(viewColumnMeta.get(i).trim());
            }
        }
    }

    private PlanNode getFirstNoMergeNode(PlanNode selNode) {
        PlanNode node = selNode.getChildren().get(0);
        if (!(node instanceof MergeNode)) {
            return node;
        }
        return getFirstNoMergeNode(node);
    }


    public String getCreateSql() {
        return createSql;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public PlanNode getViewQuery() {
        return viewQuery;
    }


    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public List<String> getViewColumnMeta() {
        return viewColumnMeta;
    }

    public void setViewColumnMeta(List<String> viewColumnMeta) {
        this.viewColumnMeta = viewColumnMeta;
    }

    public String getViewColumnMetaString() {
        if (viewColumnMeta != null) {
            StringBuffer sb = new StringBuffer("(");
            for (String s : viewColumnMeta) {
                sb.append(s);
                sb.append(",");
            }
            sb.setCharAt(sb.length() - 1, ')');
            return sb.toString();
        }
        return null;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
