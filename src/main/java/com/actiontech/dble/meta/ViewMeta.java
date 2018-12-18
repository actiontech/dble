/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.actiontech.dble.config.ErrorCode.CREATE_VIEW_ERROR;

/**
 * Created by szf on 2017/9/29.
 */
public class ViewMeta {
    private String createSql;
    private String selectSql;
    private String viewName;
    private QueryNode viewQuery;


    private List<String> viewColumnMeta;
    private String schema;
    private ProxyMetaManager tmManager;


    public ErrorPacket init(boolean isReplace) {

        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        try {
            viewParser.parseCreateView(this);
            //check if the select part has
            this.checkDuplicate(viewParser, isReplace);

            SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);

            MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(this.schema, 63, tmManager, false);
            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            selNode.setUpFields();

            //set the view column name into
            this.setFieldsAlias(selNode);

            viewQuery = new QueryNode(selNode);
        } catch (Exception e) {
            //the select part sql is wrong & report the error
            ErrorPacket error = new ErrorPacket();
            error.setMessage(e.getMessage() == null ? "unknow error".getBytes(StandardCharsets.UTF_8) :
                    e.getMessage().getBytes(StandardCharsets.UTF_8));
            error.setErrNo(CREATE_VIEW_ERROR);
            return error;
        }
        return null;
    }


    public ErrorPacket initAndSet(boolean isReplace, boolean isNewCreate) {

        //check the create sql is legal
        //parse sql into three parts
        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        viewParser.parseCreateView(this);

        try {
            if ("".equals(viewName)) {
                throw new Exception("sql not supported ");
            }

            tmManager.addMetaLock(schema, viewName, createSql);

            //check if the select part has
            this.checkDuplicate(viewParser, isReplace);

            SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);

            MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(this.schema, 63, tmManager, false);

            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            selNode.setUpFields();

            //set the view column name into
            this.setFieldsAlias(selNode);

            viewQuery = new QueryNode(selNode);

            if (isNewCreate) {
                DbleServer.getInstance().getTmManager().getRepository().put(schema, viewName, this.createSql);
            }

            tmManager.getCatalogs().get(schema).getViewMetas().put(viewName, this);
        } catch (Exception e) {
            //the select part sql is wrong & report the error
            ErrorPacket error = new ErrorPacket();
            error.setMessage(e.getMessage() == null ? "unknown error".getBytes(StandardCharsets.UTF_8) :
                    e.getMessage().getBytes(StandardCharsets.UTF_8));
            error.setErrNo(CREATE_VIEW_ERROR);
            return error;
        } finally {
            tmManager.removeMetaLock(schema, viewName);
        }
        return null;
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


    private void setFieldsAlias(PlanNode selNode) throws Exception {
        if (viewColumnMeta != null) {
            //check if the column number of view is same as the selectList in selectStatement
            if (viewColumnMeta.size() != selNode.getColumnsSelected().size()) {
                //return error
                throw new Exception("The Column_list Size and Select_statement Size Not Match");
            }
            for (int i = 0; i < viewColumnMeta.size(); i++) {
                selNode.getColumnsSelected().get(i).setAlias(viewColumnMeta.get(i).trim());
            }
        } else {
            List<Item> selectList = selNode.getColumnsSelected();
            Set<String> tempMap = new HashSet<String>();
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
        }
    }


    /**
     * get the select part of view create sql
     *
     * @return
     */
    public String parseSelect() {
        return null;
    }

    public ViewMeta(String createSql, String schema, ProxyMetaManager tmManager) {
        this.createSql = createSql;
        this.schema = schema;
        this.tmManager = tmManager;
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

    public QueryNode getViewQuery() {
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

    public void setViewColumnMeta(List<String> viewColumnMeta) {
        this.viewColumnMeta = viewColumnMeta;
    }

}
