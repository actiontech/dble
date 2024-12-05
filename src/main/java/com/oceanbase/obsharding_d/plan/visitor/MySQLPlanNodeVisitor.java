/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.visitor;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemField;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemCreate;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondAnd;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemAllAnySubQuery;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemExistsSubQuery;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemInSubQuery;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemScalarSubQuery;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;
import com.oceanbase.obsharding_d.plan.node.*;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.oceanbase.obsharding_d.plan.util.FilterUtils;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseView;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import javax.annotation.Nullable;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType.INNER_JOIN;

public class MySQLPlanNodeVisitor {
    protected final String currentDb;
    protected final int charsetIndex;
    protected final ProxyMetaManager metaManager;
    protected PlanNode tableNode;
    protected boolean containSchema = false;
    protected boolean isSubQuery = false;
    protected Map<String, String> usrVariables;
    protected HintPlanInfo hintPlanInfo;

    public MySQLPlanNodeVisitor(String currentDb, int charsetIndex, ProxyMetaManager metaManager, boolean isSubQuery, Map<String, String> usrVariables, @Nullable HintPlanInfo hintPlanInfo) {
        this.currentDb = currentDb;
        this.charsetIndex = charsetIndex;
        this.metaManager = metaManager;
        this.isSubQuery = isSubQuery;
        this.usrVariables = usrVariables;
        this.hintPlanInfo = hintPlanInfo;
    }

    public PlanNode getTableNode() {
        return tableNode;
    }

    public boolean isContainSchema() {
        return containSchema;
    }

    public boolean visit(SQLSelectStatement node) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("visit-for-sql-structure");
        try {
            SQLSelectQuery sqlSelect = node.getSelect().getQuery();
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(sqlSelect);
            this.tableNode = mtv.getTableNode();
            this.containSchema = mtv.isContainSchema();
            MySQLItemVisitor.clearCache();
            return true;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void visit(SQLSelectQuery node) {
        if (node instanceof MySqlSelectQueryBlock) {
            visit((MySqlSelectQueryBlock) node);
        } else if (node instanceof SQLUnionQuery) {
            visit((SQLUnionQuery) node);
        }
    }

    public boolean visit(SQLUnionQuery sqlSelectQuery) {
        MergeNode mergeNode = new MergeNode();
        SQLSelectQuery left = sqlSelectQuery.getLeft();
        MySQLPlanNodeVisitor mtvLeft = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
        mtvLeft.visit(left);
        mergeNode.addChild(mtvLeft.getTableNode());
        mergeNode.setContainsSubQuery(mtvLeft.getTableNode().isContainsSubQuery());
        BoolPtr containSchemaPtr = new BoolPtr(mtvLeft.isContainSchema());
        mergeNode = checkRightChild(mergeNode, sqlSelectQuery.getRight(), isUnion(sqlSelectQuery.getOperator()), containSchemaPtr);
        this.tableNode = mergeNode;
        this.containSchema = containSchemaPtr.get();
        SQLOrderBy orderBy = sqlSelectQuery.getOrderBy();
        if (orderBy != null) {
            handleOrderBy(orderBy);
        }
        SQLLimit limit = sqlSelectQuery.getLimit();
        if (limit != null) {
            handleLimit(limit);
        }
        return true;
    }

    public boolean visit(MySqlSelectQueryBlock sqlSelectQuery) {
        SQLTableSource from = sqlSelectQuery.getFrom();
        if (from != null) {
            String innerFuncSelectSQL = createInnerFuncSelectSQL(sqlSelectQuery.getSelectList());
            if (innerFuncSelectSQL != null) {
                MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
                mtv.visit(from);
                NoNameNode innerNode = new NoNameNode(currentDb, innerFuncSelectSQL);
                innerNode.setFakeNode(true);
                innerNode.select(handleSelectItems(selectInnerFuncList(sqlSelectQuery.getSelectList())));
                this.tableNode = new JoinInnerNode(innerNode, mtv.getTableNode());
                this.containSchema = mtv.isContainSchema();
            } else {
                visit(from);
            }
            if (this.tableNode instanceof NoNameNode) {
                this.tableNode.setSql(SQLUtils.toMySqlString(sqlSelectQuery));
            }
        } else {
            this.tableNode = new NoNameNode(currentDb, SQLUtils.toMySqlString(sqlSelectQuery));
            String innerFuncSelectSQL = createInnerFuncSelectSQL(sqlSelectQuery.getSelectList());
            if (innerFuncSelectSQL != null) {
                ((NoNameNode) tableNode).setFakeNode(true);
            }
        }

        if (tableNode != null && (sqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT || sqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW)) {
            this.tableNode.setDistinct(true);
        }

        List<SQLSelectItem> items = sqlSelectQuery.getSelectList();
        if (items != null) {
            this.tableNode.select(handleSelectItems(items));
        }

        SQLExpr whereExpr = sqlSelectQuery.getWhere();
        if (whereExpr != null) {
            handleWhereCondition(whereExpr);
        }

        SQLOrderBy orderBy = sqlSelectQuery.getOrderBy();
        if (orderBy != null) {
            handleOrderBy(orderBy);
        }

        SQLSelectGroupByClause groupBy = sqlSelectQuery.getGroupBy();
        if (groupBy != null) {
            handleGroupBy(groupBy);
        }

        SQLLimit limit = sqlSelectQuery.getLimit();
        if (limit != null) {
            handleLimit(limit);
        }
        return true;
    }

    public boolean visit(SQLExprTableSource tableSource) {
        PlanNode table;
        String schema;
        String tableName;
        SQLExpr expr = tableSource.getExpr();
        if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
            schema = StringUtil.removeBackQuote(propertyExpr.getOwnerName());
            tableName = StringUtil.removeBackQuote(propertyExpr.getName());
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
                tableName = tableName.toLowerCase();
            }
            containSchema = true;
        } else if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            if (identifierExpr.getName().equalsIgnoreCase("dual")) {
                this.tableNode = new NoNameNode(currentDb, null);
                return true;
            }
            schema = currentDb;
            if (schema == null) {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "3D000", "No database selected");
            }
            tableName = StringUtil.removeBackQuote(identifierExpr.getName());
            if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                schema = schema.toLowerCase();
                tableName = tableName.toLowerCase();
            }
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "table is " + tableSource.toString());
        }

        if (metaManager != null) {
            //here to check if the table name is a view in metaManager
            PlanNode viewNode;
            try {
                viewNode = metaManager.getSyncView(schema, tableName);
            } catch (SQLNonTransientException e) {
                throw new MySQLOutPutException(e.getErrorCode(), e.getSQLState(), e.getMessage());
            }
            if (viewNode != null) {
                //consider if the table with other name
                viewNode.setAlias(tableSource.getAlias() == null ? tableName : tableSource.getAlias());
                this.tableNode = viewNode;
                if (viewNode instanceof QueryNode) {
                    tableNode.setWithSubQuery(true);
                    tableNode.setExistView(true);
                    tableNode.setKeepFieldSchema(false);
                }
                return true;
            } else {
                try {
                    table = new TableNode(schema, tableName, this.metaManager, charsetIndex);
                } catch (SQLNonTransientException e) {
                    throw new MySQLOutPutException(e.getErrorCode(), e.getSQLState(), e.getMessage());
                }
                ((TableNode) table).setHintList(tableSource.getHints());
                this.tableNode = table;
                return true;
            }
        } else {

            ManagerBaseView view = ManagerSchemaInfo.getInstance().getView(tableName);
            if (view != null) {
                this.tableNode = view.getViewNode().copy();
                return true;
            }

            try {
                table = new ManagerTableNode(schema, tableName);
            } catch (SQLNonTransientException e) {
                throw new MySQLOutPutException(e.getErrorCode(), e.getSQLState(), e.getMessage());
            }
            this.tableNode = table;
            return true;
        }
    }

    public boolean visit(SQLUnionQueryTableSource unionTables) {
        visit(unionTables.getUnion());
        this.tableNode.setWithSubQuery(true);
        if (unionTables.getAlias() != null) {
            tableNode.setAlias(unionTables.getAlias());
        }
        return true;
    }

    public boolean visit(SQLJoinTableSource joinTables) {
        SQLTableSource left = joinTables.getLeft();
        MySQLPlanNodeVisitor mtvLeft = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
        mtvLeft.visit(left);

        SQLTableSource right = joinTables.getRight();
        MySQLPlanNodeVisitor mtvRight = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
        mtvRight.visit(right);
        JoinNode joinNode = new JoinNode(mtvLeft.getTableNode(), mtvRight.getTableNode(), this.charsetIndex);
        joinNode.setContainsSubQuery(mtvLeft.getTableNode().isContainsSubQuery() || mtvRight.getTableNode().isContainsSubQuery());

        tryChangeJoinType(joinTables);

        switch (joinTables.getJoinType()) {
            case JOIN:
            case CROSS_JOIN:
            case INNER_JOIN:
            case STRAIGHT_JOIN:
                joinNode.setInnerJoin();
                break;
            case LEFT_OUTER_JOIN:
                if ((joinTables.getCondition() == null) && (joinTables.getUsing() == null || joinTables.getUsing().size() == 0) && !joinTables.isNatural()) {
                    throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "left join without join_condition!");
                }
                joinNode.setLeftOuterJoin();
                break;
            case RIGHT_OUTER_JOIN:
                if ((joinTables.getCondition() == null) && (joinTables.getUsing() == null || joinTables.getUsing().size() == 0) && !joinTables.isNatural()) {
                    throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "right join without join_condition!");
                }
                joinNode.setRightOuterJoin();
                break;
            case NATURAL_JOIN:// never happen
                break;
            default:
                break;
        }
        int condTypeCnt = 0;
        if (joinTables.isNatural()) {
            condTypeCnt++;
            joinNode.setNatural(true);
        }
        SQLExpr cond = joinTables.getCondition();
        if (cond != null) {
            condTypeCnt++;
            if (condTypeCnt > 1) {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000",
                        "You have an error in your SQL syntax;check the manual that corresponds to your MySQL server version for the right syntax to use near '" + cond.toString() + "'");
            }
            MySQLItemVisitor ev = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
            cond.accept(ev);
            addJoinOnColumns(ev.getItem(), joinNode);
        }
        if (joinTables.getUsing() != null && joinTables.getUsing().size() != 0) {
            condTypeCnt++;
            if (condTypeCnt > 1) {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000",
                        "You have an error in your SQL syntax;check the manual that corresponds to your MySQL server version for the right syntax to use near '" + joinTables.getUsing().toString() + "'");
            }
            joinNode.setUsingFields(this.getUsingFields(joinTables.getUsing()));
        }
        this.tableNode = joinNode;
        this.containSchema = mtvLeft.isContainSchema() || mtvRight.isContainSchema();
        return true;
    }

    public boolean visit(SQLSubqueryTableSource subQueryTables) {
        SQLSelect sqlSelect = subQueryTables.getSelect();
        visit(sqlSelect.getQuery());
        return true;
    }

    public boolean visit(SQLSelect node) {
        MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
        mtv.visit(node.getQuery());
        this.tableNode = mtv.getTableNode();
        this.containSchema = mtv.isContainSchema();
        return true;
    }


    public void visit(SQLTableSource tables) {
        if (tables instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(table);
            this.tableNode = mtv.getTableNode();
            this.containSchema = mtv.isContainSchema();
        } else if (tables instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTables = (SQLJoinTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(joinTables);
            this.tableNode = mtv.getTableNode();
            this.containSchema = mtv.isContainSchema();
        } else if (tables instanceof SQLUnionQueryTableSource) {
            if (tables.getAlias() == null) {
                throw new MySQLOutPutException(ErrorCode.ER_DERIVED_MUST_HAVE_ALIAS, "", "Every derived table must have its own alias");
            }
            SQLUnionQueryTableSource unionTables = (SQLUnionQueryTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(unionTables);
            this.tableNode = new QueryNode(mtv.getTableNode());
            this.tableNode.setContainsSubQuery(mtv.getTableNode().isContainsSubQuery());
            this.containSchema = mtv.isContainSchema();
        } else if (tables instanceof SQLSubqueryTableSource) {
            if (tables.getAlias() == null) {
                throw new MySQLOutPutException(ErrorCode.ER_DERIVED_MUST_HAVE_ALIAS, "", "Every derived table must have its own alias");
            }
            SQLSubqueryTableSource subQueryTables = (SQLSubqueryTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(subQueryTables);
            this.tableNode = new QueryNode(mtv.getTableNode());
            this.tableNode.setSql(subQueryTables.toString());
            this.tableNode.setContainsSubQuery(mtv.getTableNode().isContainsSubQuery());
            this.containSchema = mtv.isContainSchema();
        }
        if (tables.getAlias() != null) {
            this.tableNode.setAlias(tables.getAlias());
        }
    }


    public void tryChangeJoinType(SQLJoinTableSource joinTables) {
        if (this.hintPlanInfo != null) {
            switch (joinTables.getJoinType()) {
                case LEFT_OUTER_JOIN:
                    if (this.hintPlanInfo.isLeft2inner()) {
                        joinTables.setJoinType(INNER_JOIN);
                    }
                    return;
                case RIGHT_OUTER_JOIN:
                    if (this.hintPlanInfo.isRight2inner()) {
                        joinTables.setJoinType(INNER_JOIN);
                    }
                    return;
                default:
                    return;
            }
        }
    }


    private List<Item> handleSelectItems(List<SQLSelectItem> items) {
        List<Item> selectItems = new ArrayList<>();
        for (SQLSelectItem item : items) {
            SQLExpr expr = item.getExpr();
            MySQLItemVisitor ev = new MySQLItemVisitor(currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
            expr.accept(ev);
            Item selItem = ev.getItem();
            selItem.setCharsetIndex(charsetIndex);
            if (selItem.isWithSubQuery()) {
                setSubQueryNode(selItem);
            }
            String alias = item.getAlias();
            if (alias != null) {
                alias = StringUtil.removeBackQuote(alias);
            }
            selItem.setAlias(alias);
            if (isSubQuery && selItem.getAlias() == null && items.size() == 1) {
                selItem.setAlias("autoalias_scalar");
            }
            selectItems.add(selItem);
        }
        return selectItems;
    }

    protected void setSubQueryNode(Item selItem) {
        if (selItem instanceof ItemScalarSubQuery) {
            ((ItemScalarSubQuery) selItem).setField(true);
            tableNode.getSubQueries().add((ItemScalarSubQuery) selItem);
            tableNode.setWithSubQuery(true);
            tableNode.setContainsSubQuery(true);
        } else if (selItem instanceof ItemAllAnySubQuery) {
            tableNode.getSubQueries().add((ItemAllAnySubQuery) selItem);
            tableNode.setWithSubQuery(true);
            tableNode.setContainsSubQuery(true);
        } else if (selItem instanceof ItemInSubQuery) {
            tableNode.getSubQueries().add((ItemInSubQuery) selItem);
            tableNode.setWithSubQuery(true);
            tableNode.setContainsSubQuery(true);
        } else if (selItem instanceof ItemExistsSubQuery) {
            ((ItemExistsSubQuery) selItem).setField(true);
            tableNode.getSubQueries().add((ItemExistsSubQuery) selItem);
            tableNode.setWithSubQuery(true);
            tableNode.setContainsSubQuery(true);
        } else if (selItem instanceof ItemFunc) {
            for (Item args : selItem.arguments()) {
                setSubQueryNode(args);
            }
        }
        tableNode.setCorrelatedSubQuery(selItem.isCorrelatedSubQuery());
    }

    protected void handleWhereCondition(SQLExpr whereExpr) {
        MySQLItemVisitor mev = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
        whereExpr.accept(mev);
        if (this.tableNode != null) {
            if (tableNode instanceof JoinInnerNode) {
                Item whereFilter = mev.getItem();
                PlanNode tn = ((JoinInnerNode) tableNode).getRightNode();
                tn.query(whereFilter);
                if (whereFilter.isWithSubQuery()) {
                    tn.setWithSubQuery(true);
                    tn.setContainsSubQuery(true);
                    tn.setCorrelatedSubQuery(whereFilter.isCorrelatedSubQuery());
                }
            } else {
                Item whereFilter = mev.getItem();
                tableNode.query(whereFilter);
                if (whereFilter.isWithSubQuery()) {
                    tableNode.setWithSubQuery(true);
                    tableNode.setContainsSubQuery(true);
                    tableNode.setCorrelatedSubQuery(whereFilter.isCorrelatedSubQuery());
                }
            }
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "from expression is null,check the sql!");
        }
    }

    protected void handleOrderBy(SQLOrderBy orderBy) {
        for (SQLSelectOrderByItem p : orderBy.getItems()) {
            SQLExpr expr = p.getExpr();
            MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
            expr.accept(v);
            if (v.getItem() instanceof ItemScalarSubQuery) {
                tableNode.setWithSubQuery(true);
                tableNode.setContainsSubQuery(true);
                tableNode.setCorrelatedSubQuery(false);
            }
            this.tableNode = tableNode.orderBy(v.getItem(), p.getType());
        }
    }

    private void handleGroupBy(SQLSelectGroupByClause groupBy) {
        for (SQLExpr p : groupBy.getItems()) {
            if (p instanceof MySqlOrderingExpr) {
                MySqlOrderingExpr groupitem = (MySqlOrderingExpr) p;
                SQLExpr q = groupitem.getExpr();
                MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
                q.accept(v);
                this.tableNode = tableNode.groupBy(v.getItem(), groupitem.getType());
            } else {
                MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
                p.accept(v);
                this.tableNode = tableNode.groupBy(v.getItem(), SQLOrderingSpecification.ASC);
            }
        }

        if (groupBy.isWithRollUp()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "with rollup is not supported yet!");
        }
        if (groupBy.getHaving() != null) {
            handleHavingCondition(groupBy.getHaving());
        }
    }


    private MergeNode checkRightChild(MergeNode mergeNode, SQLSelectQuery rightQuery, boolean isUnion, BoolPtr containSchemaPtr) {
        if (rightQuery instanceof SQLUnionQuery) {
            SQLUnionQuery subUnion = (SQLUnionQuery) rightQuery;
            SQLSelectQuery subLeft = subUnion.getLeft();
            MySQLPlanNodeVisitor mtvRight = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtvRight.visit(subLeft);
            mergeNode.addChild(mtvRight.getTableNode());
            containSchemaPtr.set(containSchemaPtr.get() || mtvRight.isContainSchema());
            mergeNode.setUnion(isUnion);

            MergeNode mergeParentNode = new MergeNode();
            mergeParentNode.addChild(mergeNode);
            mergeParentNode.setContainsSubQuery(mergeNode.isContainsSubQuery());
            return checkRightChild(mergeParentNode, subUnion.getRight(), isUnion(subUnion.getOperator()), containSchemaPtr);

        } else {
            MySQLPlanNodeVisitor mtvRight = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtvRight.visit(rightQuery);
            mergeNode.addChild(mtvRight.getTableNode());
            mergeNode.setUnion(isUnion);
            containSchemaPtr.set(containSchemaPtr.get() || mtvRight.isContainSchema());
            return mergeNode;
        }
    }

    private boolean isUnion(SQLUnionOperator unionOperator) {
        if (unionOperator == SQLUnionOperator.UNION || unionOperator == SQLUnionOperator.DISTINCT) {
            return true;
        } else if (unionOperator == SQLUnionOperator.UNION_ALL) {
            return false;
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "You have an error in your SQL syntax;" + unionOperator.toString());
        }
    }

    private void handleHavingCondition(SQLExpr havingExpr) {
        MySQLItemVisitor mev = new MySQLItemVisitor(currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
        havingExpr.accept(mev);
        Item havingFilter = mev.getItem();
        if (this.tableNode == null) {
            throw new IllegalArgumentException("from expression is null,check the sql!");
        }
        if (havingFilter.isWithSubQuery()) {
            tableNode.setWithSubQuery(true);
            tableNode.setContainsSubQuery(true);
            tableNode.setCorrelatedSubQuery(havingFilter.isCorrelatedSubQuery());
        }
        this.tableNode = this.tableNode.having(havingFilter);

    }

    private void handleLimit(SQLLimit limit) {
        long from = 0;
        SQLExpr offset = limit.getOffset();
        if (offset != null) {
            SQLIntegerExpr offsetExpr = (SQLIntegerExpr) offset;
            from = offsetExpr.getNumber().longValue();
        }
        SQLExpr rowCount = limit.getRowCount();
        long to = ((SQLIntegerExpr) rowCount).getNumber().longValue();
        if (to < 0) {
            throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '" + to + "'");
        }
        tableNode.setLimitFrom(from);
        tableNode.setLimitTo(to);
    }

    protected void addJoinOnColumns(Item ifilter, JoinNode joinNode) {
        if (ifilter instanceof ItemFuncEqual) {
            ItemFuncEqual filter = (ItemFuncEqual) ifilter;
            Item column = filter.arguments().get(0);
            Item value = filter.arguments().get(1);
            if (column != null && column instanceof ItemField && value != null && value instanceof ItemField) {
                joinNode.addJoinFilter(filter);
            } else {
                Item orgOtherJoin = joinNode.getOtherJoinOnFilter();
                joinNode.setOtherJoinOnFilter(FilterUtils.and(orgOtherJoin, filter));
            }
        } else if (ifilter instanceof ItemCondAnd) {
            ItemCondAnd ilfand = (ItemCondAnd) ifilter;
            List<Item> subFilter = ilfand.arguments();
            if (subFilter != null) {
                for (Item arg : subFilter) {
                    addJoinOnColumns(arg, joinNode);
                }
            } else {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "and has no other columns , " + ifilter);
            }

        } else {
            Item orgOtherJoin = joinNode.getOtherJoinOnFilter();
            joinNode.setOtherJoinOnFilter(FilterUtils.and(orgOtherJoin, ifilter));
        }
    }

    protected List<String> getUsingFields(List<SQLExpr> using) {
        List<String> fds = new ArrayList<>(using.size());
        for (SQLExpr us : using) {
            fds.add(StringUtil.removeBackQuote(us.toString().toLowerCase()));
        }
        return fds;
    }

    private String createInnerFuncSelectSQL(List<SQLSelectItem> items) {
        StringBuffer sb = new StringBuffer("SELECT ");
        if (items != null) {
            for (SQLSelectItem si : items) {
                if (si.getExpr() instanceof SQLMethodInvokeExpr &&
                        ItemCreate.getInstance().isInnerFunc(((SQLMethodInvokeExpr) si.getExpr()).getMethodName())) {
                    sb.append(si.getExpr().toString() + ",");
                }
            }
            if (sb.length() > 7) {
                sb.setLength(sb.length() - 1);
                return sb.toString();
            }
        }
        return null;
    }

    private List<SQLSelectItem> selectInnerFuncList(List<SQLSelectItem> items) {
        List<SQLSelectItem> result = new ArrayList<>();
        for (SQLSelectItem si : items) {
            if (si.getExpr() instanceof SQLMethodInvokeExpr &&
                    ItemCreate.getInstance().isInnerFunc(((SQLMethodInvokeExpr) si.getExpr()).getMethodName())) {
                result.add(si);
            }
        }
        return result;
    }

}
