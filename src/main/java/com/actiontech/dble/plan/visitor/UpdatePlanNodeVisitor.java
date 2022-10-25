/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.visitor;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.subquery.UpdateItemSubQuery;
import com.actiontech.dble.plan.node.ModifyNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdatePlanNodeVisitor extends MySQLPlanNodeVisitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePlanNodeVisitor.class);


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
                handleWhereCondition(whereExpr);
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
        for (ItemFuncEqual itemFuncEqual : query.getSetItemList()) {
            setValItemList.add(itemFuncEqual.arguments().get(1));
        }

        //only supports update set single table
        StringBuilder setBuilder = new StringBuilder();
        String tableName = null;
        for (Item item : setValItemList) {
            if (tableName == null) {
                tableName = item.getTableName();
                if (!StringUtil.isEmpty(item.getTableName())) {
                    setBuilder.append("`" + item.getTableName() + "`.`" + item.getItemName() + "`,");
                }
            } else if (!tableName.equals(item.getTableName())) {
                throw new MySQLOutPutException(ErrorCode.ERR_NOT_SUPPORTED, "", "Update set multiple tables is not supported yet!");
            } else {
                setBuilder.append("`" + item.getTableName() + "`.`" + item.getItemName() + "`,");
            }
        }

        setBuilder.deleteCharAt(setBuilder.length() - 1);
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ");
        //set
        sqlBuilder.append(setBuilder);
        //where
        StringBuilder whereBuilder = new StringBuilder();
        String finalTableName = tableName;

        List<Item> whereItemList = getAllWhereItem(query.getWhereFilter());
        whereItemList.stream().forEach(whereItem -> {
            if (!StringUtil.isEmpty(whereItem.getTableName()) && whereItem.getTableName().equals(finalTableName)) {
                whereBuilder.append(",`" + whereItem.getTableName() + "`.`" + whereItem.getItemName() + "` ");
            }
        });
        sqlBuilder.append(whereBuilder);
        sqlBuilder.append(" from ");

        List<TableNode> tableNodes = query.getReferedTableNodes();
        for (TableNode tableNode : tableNodes) {
            if (!StringUtil.isEmpty(tableName) && tableName.equals(tableNode.getAlias())) {
                buildTableName(tableNode, sqlBuilder);
            }
        }
        if (!whereBuilder.toString().isEmpty()) {
            whereBuilder.deleteCharAt(0);
            sqlBuilder.append(" group by ");
            sqlBuilder.append(whereBuilder);
        }
        return sqlBuilder.toString();
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
            modifyNode.addSetItem(itemFuncEqual);
        }
    }

    protected void handleWhereCondition(SQLExpr whereExpr) {
        MySQLItemVisitor mev = new MySQLItemVisitor(this.currentDb, this.charsetIndex, this.metaManager, this.usrVariables, this.hintPlanInfo);
        whereExpr.accept(mev);
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
