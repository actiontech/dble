package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.List;

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
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) selectStatement.getSelect().getQuery();

            //set the view column name into
            if (viewColumnMeta != null) {
                //check if the column number of view is same as the selectList in selectStatement
                if (viewColumnMeta.size() != selectQueryBlock.getSelectList().size()) {
                    //return error
                    throw new Exception("The Column_list Size and Select_statement Size Not Match");
                }
                for (int i = 0; i < viewColumnMeta.size(); i++) {
                    selectQueryBlock.getSelectList().get(i).setAlias(viewColumnMeta.get(i).trim());
                }
            }
            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            //进行一次优化
            viewQuery = new QueryNode(selNode);
        } catch (Exception e) {
            //the select part sql is wrong & report the error
            ErrorPacket error = new ErrorPacket();
            error.setMessage(e.getMessage().getBytes());
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
