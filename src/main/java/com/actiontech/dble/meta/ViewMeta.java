/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

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

    public void init(boolean isReplace, boolean isMysqlView) throws Exception {
        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        viewParser.parseCreateView(this);
        //check if the select part has
        this.checkDuplicate(viewParser, isReplace);
        this.parseSelectInView(isMysqlView);
    }

    private void parseSelectInView(boolean isMysqlView) throws Exception {
        SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);
        if (isMysqlView) {
            List<SQLSelectItem> selectItems = ((SQLSelectQueryBlock) selectStatement.getSelect().getQuery()).getSelectList();
            viewColumnMeta = new ArrayList<>(selectItems.size());
            for (SQLSelectItem item : selectItems) {
                String alias = item.getAlias() == null ? item.getExpr().toString() : item.getAlias();
                viewColumnMeta.add(StringUtil.removeBackQuote(alias));
            }
            viewQuery = new TableNode(schema, viewName, viewColumnMeta);
        } else {
            MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(this.schema, 63, tmManager, false);
            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            if (selNode instanceof MergeNode) {
                this.setFieldsAlias(selNode, true);
                selNode.setUpFields();
            } else {
                selNode.setUpFields();
                this.setFieldsAlias(selNode, false);
            }
            viewQuery = new QueryNode(selNode);
        }
    }

    public void initAndSet(boolean isReplace, boolean isNewCreate, boolean isMysqlView) throws Exception {
        //check the create sql is legal
        //parse sql into three parts
        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        viewParser.parseCreateView(this);
        if ("".equals(viewName)) {
            throw new Exception("sql not supported ");
        }

        try {
            tmManager.addMetaLock(schema, viewName, createSql);
            //check if the select part has
            checkDuplicate(viewParser, isReplace);
            parseSelectInView(isMysqlView);
            if (isNewCreate) {
                ProxyMeta.getInstance().getTmManager().getRepository().put(schema, viewName, this.createSql);
            }
            tmManager.getCatalogs().get(schema).getViewMetas().put(viewName, this);
        } finally {
            tmManager.removeMetaLock(schema, viewName);
        }
    }

    private void checkDuplicate(ViewMetaParser viewParser, Boolean isReplace) throws Exception {

        ViewMeta viewNode = tmManager.getCatalogs().get(schema).getViewMetas().get(viewName);
        //.getSyncView(schema,viewName);
        StructureMeta.TableMeta tableMeta = tmManager.getCatalogs().get(schema).getTableMeta(viewName);
        //if the alter table
        if (viewParser.getType() == ViewMetaParser.TYPE_ALTER_VIEW && !isReplace) {
            if (viewNode == null) {
                throw new Exception("Table '" + viewName + "' doesn't exist");
            }
        }

        if (viewColumnMeta != null) {
            Set<String> tempMap = new HashSet<String>();
            for (String t : viewColumnMeta) {
                if (tempMap.contains(t.trim())) {
                    throw new Exception("Duplicate column name '" + t + "'");
                }
                tempMap.add(t.trim());
            }
        }

        // if the table with same name exists
        if (tableMeta != null) {
            throw new Exception("Table '" + viewName + "' already exists");
        }

        if (viewParser.getType() == ViewMetaParser.TYPE_CREATE_VIEW && !isReplace) {
            // if the sql without replace & the view exists
            if (viewNode != null) {
                // return error because the view is exists
                throw new Exception("Table '" + viewName + "' already exists");
            }
        }
    }

    private void setFieldsAlias(PlanNode selNode, boolean isMergeNode) throws Exception {
        if (viewColumnMeta == null) {
            List<Item> selectList = selNode.getColumnsSelected();
            Set<String> tempMap = new HashSet<>();
            for (Item t : selectList) {
                if (t.getAlias() != null) {
                    if (tempMap.contains(t.getAlias())) {
                        throw new Exception("Duplicate column name '" + t.getItemName() + "'");
                    }
                    tempMap.add(t.getAlias());
                } else {
                    if (tempMap.contains(t.getItemName())) {
                        throw new Exception("Duplicate column name '" + t.getItemName() + "'");
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
            throw new Exception("The Column_list Size and Select_statement Size Not Match");
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

    /**
     * get the select part of view create sql
     *
     * @return
     */
    public String parseSelect() {
        return null;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
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

    public void setViewQuery(QueryNode viewQuery) {
        this.viewQuery = viewQuery;
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
}
