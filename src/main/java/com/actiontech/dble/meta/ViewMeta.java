package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.nio.charset.StandardCharsets;
import java.util.*;

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


    public ErrorPacket init(boolean isReplace) {

        //check the create sql is legal
        //parse sql into three parts
        ViewMetaParser viewParser = new ViewMetaParser(createSql);
        viewParser.parseCreateView(this);
        try {
            SchemaMeta schemaMeta = DbleServer.getInstance().getTmManager().getCatalogs().get(schema);

            //if the alter table
            if (viewParser.getType() == ViewMetaParser.TYPE_ALTER_VIEW && !isReplace) {
                if (schemaMeta.getView(viewName) == null) {
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
            if (schemaMeta.getTableMeta(viewName) != null) {
                throw new Exception("Table '" + viewName + "' already exists");
            }

            if (viewParser.getType() == ViewMetaParser.TYPE_CREATE_VIEW) {
                // if the sql without replace & the view exists
                if (schemaMeta.getView(viewName) != null) {
                    // return error because the view is exists
                    throw new Exception("Table '" + viewName + "' already exists");
                }
            }

            SQLSelectStatement selectStatement = (SQLSelectStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(selectSql);

            MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(this.schema, 63);

            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            selNode.setUpFields();

            //check if the select part has

            //set the view column name into
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
                    if (tempMap.contains(t.getItemName())) {
                        throw new Exception("Duplicate column name '" + t.getItemName() + "'");
                    }
                    tempMap.add(t.getItemName());
                }
            }

            viewQuery = new QueryNode(selNode);
        } catch (Exception e) {
            //the select part sql is wrong & report the error
            ErrorPacket error = new ErrorPacket();
            error.setMessage(e.getMessage().getBytes(StandardCharsets.UTF_8));
            error.setErrNo(CREATE_VIEW_ERROR);
            return error;
        }
        return null;
    }


    /**
     * get the select part of view create sql
     *
     * @return
     */
    public String parseSelect() {
        return null;
    }

    public ViewMeta(String createSql, String schema) {
        this.createSql = createSql;
        this.schema = schema;
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

    public void setViewColumnMeta(List<String> viewColumnMeta) {
        this.viewColumnMeta = viewColumnMeta;
    }

}
