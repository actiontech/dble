/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.visitor;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemBasicConstant;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondAnd;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondOr;
import com.oceanbase.obsharding_d.plan.common.item.function.sumfunc.ItemSum;
import com.oceanbase.obsharding_d.plan.common.item.subquery.UpdateItemSubQuery;
import com.oceanbase.obsharding_d.plan.node.ModifyNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.QueryNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanInfo;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.parser.druid.impl.DruidUpdateParser;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.oceanbase.obsharding_d.util.StringUtil.equalsIgnoreCase;

public class UpdatePlanNodeVisitor extends MySQLPlanNodeVisitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePlanNodeVisitor.class);

    private static final String ERROR_MSG = "Update set multiple tables is not supported yet!";


    public UpdatePlanNodeVisitor(String currentDb, int charsetIndex, ProxyMetaManager metaManager, boolean isSubQuery, Map<String, String> usrVariables, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, charsetIndex, metaManager, isSubQuery, usrVariables, hintPlanInfo);
    }

    public boolean visit(SQLUpdateStatement node) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("visit-for-sql-structure");
        try {
            if (node instanceof MySqlUpdateStatement) {
                return visit((MySqlUpdateStatement) node);
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
        return false;
    }


    public boolean visit(MySqlUpdateStatement node) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("visit-for-sql-structure");
        try {
            UpdatePlanNodeVisitor mtv = new UpdatePlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(node.getTableSource());
            ModifyNode modifyNode = new ModifyNode(mtv.getTableNode());

            this.tableNode = modifyNode;
            this.containSchema = mtv.isContainSchema();

            List<SQLUpdateSetItem> items = node.getItems();
            if (items != null) {
                handleSetItem(modifyNode, items);
            }

            SQLExpr whereExpr = node.getWhere();
            if (whereExpr != null) {
                handleCondition(whereExpr);
            }
            if (node.getTableSource() instanceof SQLJoinTableSource) {
                SQLJoinTableSource joinTableSource = (SQLJoinTableSource) node.getTableSource();
                SQLExpr condition = joinTableSource.getCondition();
                if (condition != null) {
                    handleCondition(condition);
                }
            }


            //https://dev.mysql.com/doc/refman/8.0/en/update.html
            SQLOrderBy orderBy = node.getOrderBy();
            if (orderBy != null) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Incorrect usage of UPDATE and ORDER");
            }
            SQLLimit limit = node.getLimit();
            if (limit != null) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Incorrect usage of UPDATE and LIMIT");
            }
            //split query
            if (modifyNode.getReferedTableNodes().size() != 2) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Update of more than 2 tables is not supported!");
            }
            //first assembly select
            assembleSelect(modifyNode);
            MySQLItemVisitor.clearCache();
            return true;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void visit(SQLTableSource tables) {
        if (tables instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource) tables;
            UpdatePlanNodeVisitor mtv = new UpdatePlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(table);
            this.tableNode = mtv.getTableNode();
            this.containSchema = mtv.isContainSchema();
        } else if (tables instanceof SQLJoinTableSource) {
            boolean equivalentWhere = ((SQLJoinTableSource) tables).getJoinType() == SQLJoinTableSource.JoinType.COMMA ||
                    ((SQLJoinTableSource) tables).getJoinType() == SQLJoinTableSource.JoinType.JOIN ||
                    ((SQLJoinTableSource) tables).getJoinType() == SQLJoinTableSource.JoinType.INNER_JOIN ||
                    ((SQLJoinTableSource) tables).getJoinType() == SQLJoinTableSource.JoinType.CROSS_JOIN;
            if (!equivalentWhere) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Update multi-table currently only supports join/inner-join/cross-join");
            }
            SQLJoinTableSource joinTables = (SQLJoinTableSource) tables;
            UpdatePlanNodeVisitor mtv = new UpdatePlanNodeVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.isSubQuery, this.usrVariables, this.hintPlanInfo);
            mtv.visit(joinTables);
            this.tableNode = mtv.getTableNode();
            this.containSchema = mtv.isContainSchema();
        }
        if (tables.getAlias() != null) {
            this.tableNode.setAlias(tables.getAlias());
        }
    }

    /**
     * assemble select to act as a sub query
     *
     * @param modifyNode
     */
    private void assembleSelect(ModifyNode modifyNode) {
        String querySql;
        PlanNode queryNode = null;
        List<PlanNode> children = modifyNode.getChildren();
        List<QueryNode> queryNodeList = children.stream()
                .filter(planNode -> planNode instanceof QueryNode)
                .map(node -> (QueryNode) node)
                .collect(Collectors.toList());
        if (!queryNodeList.isEmpty()) {
            if (queryNodeList.size() > 1) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Number of subqueries greater than 1 is not supported!");
            }
            querySql = queryNodeList.get(0).getSql();
            queryNode = queryNodeList.get(0);
        } else {
            querySql = handleQuery(modifyNode);
        }
        LOGGER.debug("merge update——query sql:{}", querySql);

        SQLStatementParser parser = new MySqlStatementParser(querySql);
        SQLSelectStatement select = (SQLSelectStatement) parser.parseStatement();
        SQLSelectQuery selectQuery = select.getSelect().getQuery();
        UpdateItemSubQuery item = new UpdateItemSubQuery(currentDb, selectQuery, null, true, metaManager, usrVariables, this.charsetIndex, this.hintPlanInfo);
        item.setQueryNode(queryNode);
        modifyNode.getSubQueries().add(item);
        modifyNode.setWithSubQuery(true);
        modifyNode.setContainsSubQuery(true);
    }

    protected String handleQuery(ModifyNode query) {
        List<Item> setValItemList = Lists.newArrayList();
        List<TableNode> tableNodes = query.getReferedTableNodes();
        Set<String> updateTableSet = Sets.newHashSet();
        for (ItemFuncEqual itemFuncEqual : query.getSetItemList()) {
            setValItemList.add(itemFuncEqual.arguments().get(1));
            updateTableSet.add(itemFuncEqual.arguments().get(0).getTableName());
        }
        if (updateTableSet.size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", ERROR_MSG);
        }

        Set<String> selectTableSet = tableNodes.stream()
                .filter(tableNode -> !updateTableSet.contains(tableNode.getAlias()))
                .map(PlanNode::getAlias)
                .collect(Collectors.toSet());

        //only supports update set single table
        Set<Item> selectColumnSet = Sets.newLinkedHashSet();
        for (Item item : setValItemList) {
            if (selectTableSet.isEmpty()) {
                selectTableSet.add(item.getTableName());
                if (!StringUtil.isEmpty(item.getTableName())) {
                    addSelectColumn(selectColumnSet, item);
                }
            } else if (!selectTableSet.contains(item.getTableName())) {
                if (updateTableSet.contains(item.getTableName())) {
                    continue;
                }
                if (!(item instanceof ItemBasicConstant)) {
                    throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", ERROR_MSG);
                }
            } else {
                addSelectColumn(selectColumnSet, item);
            }
        }

        List<Item> whereItemList = getAllWhereItem(query.getWhereFilter());
        whereItemList.forEach(whereItem -> {
            if (!StringUtil.isEmpty(whereItem.getTableName()) && selectTableSet.contains(whereItem.getTableName())) {
                addSelectColumn(selectColumnSet, whereItem);
            }
        });

        if (selectColumnSet.isEmpty()) {
            throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", DruidUpdateParser.MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }

        StringBuilder selectBuilder = new StringBuilder();
        for (Item item : selectColumnSet) {
            selectBuilder.append("`").append(item.getTableName()).append("`.`").append(item.getItemName()).append("`,");
        }
        selectBuilder.deleteCharAt(selectBuilder.length() - 1);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ").append(selectBuilder).append(" from");
        for (TableNode tableNode : tableNodes) {
            if (selectTableSet.contains(tableNode.getAlias())) {
                buildTableName(tableNode, sqlBuilder);
            }
        }
        sqlBuilder.append(" group by ");
        sqlBuilder.append(selectBuilder);
        return sqlBuilder.toString();
    }

    private void addSelectColumn(Set<Item> selectColumnSet, Item item) {
        boolean isRepeat = selectColumnSet.stream()
                .anyMatch(selectColumn -> equalsIgnoreCase(selectColumn.getTableName(), item.getTableName()) &&
                        equalsIgnoreCase(selectColumn.getItemName(), item.getItemName()));
        if (!isRepeat) {
            selectColumnSet.add(item);
        }
    }

    private List<Item> getAllWhereItem(Item item) {
        List<Item> whereItemList = Lists.newArrayList();
        if (PlanUtil.isCmpFunc(item)) {
            whereItemList.addAll(item.arguments());
            return whereItemList;
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            for (int index = 0; index < item.getArgCount(); index++) {
                whereItemList.addAll(getAllWhereItem(item.arguments().get(index)));
            }
        }
        return whereItemList;
    }

    void buildTableName(TableNode tableNode, StringBuilder sb) {
        String tableName = "`" + tableNode.getPureName() + "`";
        sb.append(" ").append(tableName);
        String alias = tableNode.getAlias();
        if (alias != null) {
            sb.append(" `").append(alias).append("`");
        }
    }

    private void handleSetItem(ModifyNode modifyNode, List<SQLUpdateSetItem> items) {
        MySQLItemVisitor mev = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
        for (SQLUpdateSetItem setItem : items) {
            setItem.accept(mev);
            ItemFuncEqual itemFuncEqual = (ItemFuncEqual) mev.getItem();
            //set-value does not support subqueries
            if (itemFuncEqual.isWithSubQuery()) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Subqueries are not supported!");
            }
            //set-value does not support expression
            boolean hasFunc = itemFuncEqual.arguments().stream().anyMatch(item -> item instanceof ItemFunc);
            if (hasFunc) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Expression not supported!");
            }
            //set-value does not support sum/min/max...
            boolean hasSum = itemFuncEqual.arguments().stream().anyMatch(item -> item instanceof ItemSum);
            if (hasSum) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", " Invalid use of group function");
            }
            //Subquery fields are not allowed to be updated
            Item setKeyItem = itemFuncEqual.arguments().get(0);
            if (StringUtil.isEmpty(setKeyItem.getTableName())) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Constants are not supported or Set an alias");
            }
            boolean setKeyNotFromSubquery = modifyNode.getReferedTableNodes().stream()
                    .anyMatch(tableNode -> (setKeyItem.getTableName().equals(tableNode.getAlias()) || setKeyItem.getTableName().equals(tableNode.getTableName())));
            if (!setKeyNotFromSubquery) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Subquery fields are not allowed to be updated");
            }
            modifyNode.addSetItem(itemFuncEqual);
        }
    }

    protected void handleCondition(SQLExpr condition) {
        MySQLItemVisitor mev = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
        condition.accept(mev);
        if (this.tableNode != null) {
            Item whereFilter = mev.getItem();
            tableNode.query(whereFilter);
            checkSupport(whereFilter);
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "from expression is null,check the sql!");
        }
    }

    private void checkSupport(Item item) {
        //where does not support subqueries
        if (item.isWithSubQuery()) {
            throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Subqueries are not supported!");
        }
        if (PlanUtil.isCmpFunc(item)) {
            //where does not support expression
            boolean hasFunc = item.arguments().stream().anyMatch(subItem -> subItem instanceof ItemFunc);
            if (hasFunc) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Expression not supported!");
            }
            //where does not support sum/min/max...
            boolean hasSum = item.arguments().stream().anyMatch(subItem -> subItem instanceof ItemSum);
            if (hasSum) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", " Invalid use of group function");
            }
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            for (Item argument : item.arguments()) {
                checkSupport(argument);
            }
        }
    }
}
