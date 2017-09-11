/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.visitor;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;

import java.util.ArrayList;
import java.util.List;

public class MySQLPlanNodeVisitor {
    private PlanNode tableNode;
    private String currentDb;
    private int charsetIndex;

    public MySQLPlanNodeVisitor(String currentDb, int charsetIndex) {
        this.currentDb = currentDb;
        this.charsetIndex = charsetIndex;
    }

    public PlanNode getTableNode() {
        return tableNode;
    }

    public boolean visit(SQLSelectStatement node) {
        SQLSelectQuery sqlSelect = node.getSelect().getQuery();
        MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtv.visit(sqlSelect);
        this.tableNode = mtv.getTableNode();
        return true;
    }

    public void visit(SQLSelectQuery node) {
        if (node instanceof MySqlSelectQueryBlock) {
            visit((MySqlSelectQueryBlock) node);
        } else if (node instanceof MySqlUnionQuery) {
            visit((MySqlUnionQuery) node);
        }
    }

    public boolean visit(SQLUnionQuery sqlSelectQuery) {
        SQLSelectQuery left = sqlSelectQuery.getLeft();
        MySQLPlanNodeVisitor mtvleft = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtvleft.visit(left);

        SQLSelectQuery right = sqlSelectQuery.getRight();
        MySQLPlanNodeVisitor mtvright = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtvright.visit(right);

        MergeNode mergeNode = new MergeNode();
        if (sqlSelectQuery.getOperator() == SQLUnionOperator.UNION || sqlSelectQuery.getOperator() == SQLUnionOperator.DISTINCT) {
            mergeNode.setUnion(true);
        }
        mergeNode.addChild(mtvleft.getTableNode());
        mergeNode.addChild(mtvright.getTableNode());
        this.tableNode = mergeNode;

        SQLOrderBy orderBy = sqlSelectQuery.getOrderBy();
        if (orderBy != null) {
            handleOrderBy(orderBy);
        }
        return true;
    }

    public boolean visit(MySqlUnionQuery sqlSelectQuery) {
        visit((SQLUnionQuery) sqlSelectQuery);
        SQLLimit limit = sqlSelectQuery.getLimit();
        if (limit != null) {
            handleLimit(limit);
        }
        return true;
    }

    public boolean visit(MySqlSelectQueryBlock sqlSelectQuery) {
        SQLTableSource from = sqlSelectQuery.getFrom();
        if (from != null) {
            visit(from);
            if (this.tableNode instanceof NoNameNode) {
                this.tableNode.setSql(SQLUtils.toMySqlString(sqlSelectQuery));
            }
        } else {
            this.tableNode = new NoNameNode(currentDb, SQLUtils.toMySqlString(sqlSelectQuery));
        }

        if (tableNode != null && (sqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT || sqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW)) {
            this.tableNode.setDistinct(true);
        }

        List<SQLSelectItem> items = sqlSelectQuery.getSelectList();
        if (items != null) {
            List<Item> selectItems = handleSelectItems(items);
            if (selectItems != null) {
                this.tableNode.select(selectItems);
            }
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
        PlanNode table = null;
        SQLExpr expr = tableSource.getExpr();
        if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
            table = new TableNode(StringUtil.removeBackQuote(propertyExpr.getOwner().toString()), StringUtil.removeBackQuote(propertyExpr.getName()));
        } else if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            if (identifierExpr.getName().equalsIgnoreCase("dual")) {
                this.tableNode = new NoNameNode(currentDb, null);
                return true;
            }
            table = new TableNode(this.currentDb, StringUtil.removeBackQuote(identifierExpr.getName()));
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "table is " + tableSource.toString());
        }
        if (tableSource.getAlias() != null) {
            table.setSubAlias(tableSource.getAlias());
        }
        ((TableNode) table).setHintList(tableSource.getHints());
        this.tableNode = table;
        return true;
    }

    public boolean visit(SQLUnionQueryTableSource unionTables) {
        visit(unionTables.getUnion());
        this.tableNode.setSubQuery(true);
        if (unionTables.getAlias() != null) {
            tableNode.alias(unionTables.getAlias());
        }
        return true;
    }

    public boolean visit(SQLJoinTableSource joinTables) {
        SQLTableSource left = joinTables.getLeft();
        MySQLPlanNodeVisitor mtvLeft = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtvLeft.visit(left);

        SQLTableSource right = joinTables.getRight();
        MySQLPlanNodeVisitor mtvRight = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtvRight.visit(right);
        JoinNode joinNode = new JoinNode(mtvLeft.getTableNode(), mtvRight.getTableNode());
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
            MySQLItemVisitor ev = new MySQLItemVisitor(this.currentDb, this.charsetIndex);
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
        return true;
    }

    public boolean visit(SQLSubqueryTableSource subQueryTables) {
        SQLSelect sqlSelect = subQueryTables.getSelect();
        visit(sqlSelect.getQuery());
        this.tableNode.setSubQuery(true);
        if (subQueryTables.getAlias() != null) {
            tableNode.alias(subQueryTables.getAlias());
        }
        return true;
    }

    public boolean visit(SQLSelect node) {
        MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
        mtv.visit(node);
        this.tableNode = mtv.getTableNode();
        return true;
    }

    public void visit(SQLTableSource tables) {
        if (tables instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
            mtv.visit(table);
            this.tableNode = mtv.getTableNode();
        } else if (tables instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTables = (SQLJoinTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
            mtv.visit(joinTables);
            this.tableNode = mtv.getTableNode();
        } else if (tables instanceof SQLUnionQueryTableSource) {
            SQLUnionQueryTableSource unionTables = (SQLUnionQueryTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
            mtv.visit(unionTables);
            this.tableNode = new QueryNode(mtv.getTableNode());
        } else if (tables instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTables = (SQLSubqueryTableSource) tables;
            MySQLPlanNodeVisitor mtv = new MySQLPlanNodeVisitor(this.currentDb, this.charsetIndex);
            mtv.visit(subQueryTables);
            this.tableNode = new QueryNode(mtv.getTableNode());
        }
    }

    private List<Item> handleSelectItems(List<SQLSelectItem> items) {
        List<Item> selectItems = new ArrayList<>();
        for (SQLSelectItem item : items) {
            SQLExpr expr = item.getExpr();
            if (expr instanceof SQLQueryExpr)
                throw new RuntimeException("query statement as column is not supported!");
            MySQLItemVisitor ev = new MySQLItemVisitor(currentDb, this.charsetIndex);
            expr.accept(ev);
            Item selItem = ev.getItem();

            selItem.setAlias(item.getAlias());
            selectItems.add(selItem);
        }
        return selectItems;
    }

    private void handleWhereCondition(SQLExpr whereExpr) {
        MySQLItemVisitor mev = new MySQLItemVisitor(this.currentDb, this.charsetIndex);
        whereExpr.accept(mev);
        if (this.tableNode != null) {
            Item whereFiler = mev.getItem();
            tableNode.query(whereFiler);
            // this.tableNode.setWhereFilter(tableNode.getWhereFilter());
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "from expression is null,check the sql!");
        }
    }

    private void handleOrderBy(SQLOrderBy orderBy) {
        for (SQLSelectOrderByItem p : orderBy.getItems()) {
            SQLExpr expr = p.getExpr();
            MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex);
            expr.accept(v);
            this.tableNode = tableNode.orderBy(v.getItem(), p.getType());
        }
    }

    private void handleGroupBy(SQLSelectGroupByClause groupBy) {
        for (SQLExpr p : groupBy.getItems()) {
            if (p instanceof MySqlOrderingExpr) {
                MySqlOrderingExpr groupitem = (MySqlOrderingExpr) p;
                SQLExpr q = groupitem.getExpr();
                MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex);
                q.accept(v);
                this.tableNode = tableNode.groupBy(v.getItem(), groupitem.getType());
            } else {
                MySQLItemVisitor v = new MySQLItemVisitor(this.currentDb, this.charsetIndex);
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

    private void handleHavingCondition(SQLExpr havingExpr) {
        MySQLItemVisitor mev = new MySQLItemVisitor(currentDb, this.charsetIndex);
        havingExpr.accept(mev);
        Item havingFilter = mev.getItem();
        if (this.tableNode == null) {
            throw new IllegalArgumentException("from expression is null,check the sql!");
        }
        this.tableNode = this.tableNode.having(havingFilter);

    }

    private void handleLimit(SQLLimit limit) {
        long from = 0;
        SQLExpr offest = limit.getOffset();
        if (offest != null) {
            SQLIntegerExpr offsetExpr = (SQLIntegerExpr) offest;
            from = offsetExpr.getNumber().longValue();
        }
        SQLExpr rowCount = limit.getRowCount();
        long to = ((SQLIntegerExpr) rowCount).getNumber().longValue();
        tableNode.setLimitFrom(from);
        tableNode.setLimitTo(to);
    }

    private void addJoinOnColumns(Item ifilter, JoinNode joinNode) {
        if (ifilter instanceof ItemFuncEqual) {
            ItemFuncEqual filter = (ItemFuncEqual) ifilter;
            Item column = filter.arguments().get(0);
            Item value = filter.arguments().get(1);
            if (column != null && column instanceof ItemField && value != null && value instanceof ItemField) {
                joinNode.addJoinFilter(filter);
            } else {
                joinNode.setOtherJoinOnFilter(filter);
            }
        } else if (ifilter instanceof ItemCondAnd) {
            ItemCondAnd ilfand = (ItemCondAnd) ifilter;
            List<Item> subFilter = ilfand.arguments();
            if (subFilter != null) {
                for (Item arg : subFilter) {
                    Item orgOtherJoin = joinNode.getOtherJoinOnFilter();
                    addJoinOnColumns(arg, joinNode);
                    joinNode.setOtherJoinOnFilter(FilterUtils.and(orgOtherJoin, joinNode.getOtherJoinOnFilter()));
                }
            } else {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "and has no other columns , " + ifilter);
            }

        } else {
            joinNode.setOtherJoinOnFilter(ifilter);
        }
    }

    private List<String> getUsingFields(List<SQLExpr> using) {
        List<String> fds = new ArrayList<>(using.size());
        for (SQLExpr us : using) {
            fds.add(StringUtil.removeBackQuote(us.toString().toLowerCase()));
        }
        return fds;
    }
}
