/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information;

import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.QueryNode;
import com.oceanbase.obsharding_d.plan.visitor.MySQLPlanNodeVisitor;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.util.Collection;
import java.util.LinkedHashMap;

public abstract class ManagerBaseView {

    private final QueryNode viewNode;
    private final String viewName;
    protected final LinkedHashMap<String, ColumnMeta> columns;
    protected final LinkedHashMap<String, Integer> columnsType;

    protected ManagerBaseView(String viewName, int filedSize, String sql) {
        this.viewName = viewName;
        this.columns = new LinkedHashMap<>(filedSize);
        this.columnsType = new LinkedHashMap<>(filedSize);
        this.initColumnAndType();
        this.viewNode = parseView(sql);
    }

    protected abstract void initColumnAndType();

    public QueryNode getViewNode() {
        return viewNode;
    }

    public String getViewName() {
        return viewName;
    }

    public Collection<ColumnMeta> getColumnsMeta() {
        return columns.values();
    }

    private QueryNode parseView(String selectSql) {
        QueryNode queryNode = null;
        try {
            SQLSelectStatement selectStatement = (SQLSelectStatement) DruidUtil.parseMultiSQL(selectSql);
            MySQLPlanNodeVisitor msv = new MySQLPlanNodeVisitor(ManagerSchemaInfo.SCHEMA_NAME, 45, null, false, null, null);
            msv.visit(selectStatement.getSelect().getQuery());
            PlanNode selNode = msv.getTableNode();
            selNode.setUpFields();
            queryNode = new QueryNode(selNode);
            queryNode.setAlias("alias");
        } catch (Exception e) {
            // ignore
            e.printStackTrace();
        }
        return queryNode;
    }
}
