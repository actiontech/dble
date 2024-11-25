/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.*;
import com.oceanbase.obsharding_d.config.privileges.ShardingPrivileges;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.parser.druid.ERRelation;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.route.util.RouterUtil;

import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.stat.TableStat;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * see http://dev.mysql.com/doc/refman/5.7/en/update.html
 *
 * @author huqing.yan
 */
public class DruidUpdateParser extends DruidModifyParser {

    public static final String MODIFY_SQL_NOT_SUPPORT_MESSAGE = "This `Complex Update Syntax` is not supported!";

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        MySqlUpdateStatement update = (MySqlUpdateStatement) stmt;
        SQLTableSource tableSource = update.getTableSource();
        String schemaName = schema == null ? null : schema.getName();
        if (tableSource instanceof SQLJoinTableSource) {
            super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
            if (visitor.getSubQueryList().size() > 0) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }

            List<SchemaInfo> schemaInfos = checkPrivilegeForModifyTable(service, schemaName, stmt, visitor.getMotifyTableSourceList());

            boolean isAllGlobal = true;
            Set<String> tableSet = Sets.newHashSet();
            for (SchemaInfo schemaInfo : schemaInfos) {
                tableSet.add(schemaInfo.getSchema() + "." + schemaInfo.getTable());
                BaseTableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
                if (tc == null || !(tc instanceof GlobalTableConfig)) {
                    isAllGlobal = false;
                }
            }

            Collection<String> routeShardingNodes;
            if (isAllGlobal) {
                routeShardingNodes = checkForMultiNodeGlobal(schemaInfos);
            } else {
                //try to route to single Node for each table
                routeShardingNodes = checkForSingleNodeTable(rrs, service.getCharset().getClient());
            }
            if (routeShardingNodes != null && !routeShardingNodes.isEmpty()) {
                if (ctx.getTables().isEmpty()) {
                    RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true, tableSet);
                } else {
                    RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true, ctx.getTables());
                }
                rrs.setFinishedRoute(true);
                return schema;
            } else {
                tryRouteAsMerge(tableSource, visitor, schemaInfos, rrs, tableSet, stmt);
                return schema;
            }
        } else {
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, (SQLExprTableSource) tableSource);
            if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), ShardingPrivileges.CheckType.UPDATE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            SchemaConfig originSchema = schema;
            schema = schemaInfo.getSchemaConfig();
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            super.visitorParse(originSchema, rrs, stmt, visitor, service, isExplain);

            String tableName = schemaInfo.getTable();
            BaseTableConfig tc = schema.getTables().get(tableName);
            String noShardingNode = RouterUtil.isNoSharding(schema, tableName);

            if (visitor.getSubQueryList().size() > 0) {
                routeForModifySubQueryList(rrs, tc, visitor, schema, service, originSchema);
                return schema;
            } else if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
                rrs.setFinishedRoute(true);
                return schema;
            }

            checkTableExists(tc, schema.getName(), tableName, ShardingPrivileges.CheckType.UPDATE);
            if (tc instanceof GlobalTableConfig) {
                if (ctx.getTables().isEmpty()) {
                    RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
                } else {
                    RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, ctx.getTables());
                }
                rrs.setFinishedRoute(true);
                return schema;
            }

            confirmColumnNotUpdated(update, tc, rrs);

            confirmChildColumnNotUpdated(update, schema, tableName);

            if (schema.getTables().get(tableName) instanceof GlobalTableConfig && ctx.getTables().size() > 1) {
                throw new SQLNonTransientException("global table is not supported in multi table related update " + tableName);
            }

            if (update.getLimit() != null) {
                this.updateAndDeleteLimitRoute(rrs, tableName, schema, service.getCharset().getClient());
            }

            if (ctx.getTables().size() == 0) {
                ctx.addTable(new Pair<>(schema.getName(), tableName));
            }
        }
        return schema;
    }

    private void tryRouteAsMerge(SQLTableSource tableSource, ServerSchemaStatVisitor visitor, List<SchemaInfo> schemaInfos, RouteResultset rrs, Set<String> tableSet, SQLStatement stmt) throws SQLNonTransientException {
        //UPDATE A,B WHERE A.shardingColumn = B.shardingColumn
        //A,B not subQuery
        boolean isContainsSubQuery = containsSubQuery(tableSource);
        if (isContainsSubQuery) {
            rrs.setSqlStatement(stmt);
            rrs.setNeedOptimizer(true);
            rrs.setFinishedRoute(true);
            return;
        }
        //A,B the association condition has an ER relationship
        boolean canMerge = canDoAsMerge(visitor, schemaInfos);
        if (!canMerge) {
            rrs.setSqlStatement(stmt);
            rrs.setNeedOptimizer(true);
            rrs.setFinishedRoute(true);
            return;
        }
        Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();
        for (Pair<String, String> table : ctx.getTables()) {
            SchemaConfig schemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(table.getKey());
            String tableName = table.getValue();
            BaseTableConfig tableConfig = schemaConfig.getTables().get(tableName);
            if (tableConfig != null && !(tableConfig instanceof GlobalTableConfig) && tablesRouteMap.get(table) == null) {
                tablesRouteMap.put(table, new HashSet<>());
                tablesRouteMap.get(table).addAll(tableConfig.getShardingNodes());
            }
        }
        Set<String> retNodesSet = RouterUtil.retainRouteMap(tablesRouteMap);
        RouterUtil.routeToMultiNode(false, rrs, retNodesSet, false, tableSet);
        rrs.setFinishedRoute(true);
    }

    private boolean containsSubQuery(SQLTableSource tableSource) {
        if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) tableSource;
            return containsSubQuery(joinTableSource.getLeft()) || containsSubQuery(joinTableSource.getRight());
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            return true;
        }
        return false;
    }

    private boolean canDoAsMerge(ServerSchemaStatVisitor visitor, List<SchemaInfo> schemaInfos) {
        List<ERRelation> erRelationList = filterERRelation(visitor.getRelationships(), schemaInfos);
        /*
          before sorting:
          a-b
          c-d
          b-c
          after sorting:
          a-b
          b-c
          c-d
         */
        Collections.sort(erRelationList);
        Set<ERTable> erTableSet = Sets.newHashSet();
        for (ERRelation erRelation : erRelationList) {
            if (erTableSet.isEmpty() || erTableSet.contains(erRelation.getLeft()) || erTableSet.contains(erRelation.getRight())) {
                erTableSet.add(erRelation.getLeft());
                erTableSet.add(erRelation.getRight());
            }
        }
        schemaInfos.removeIf(schemaInfo -> erTableSet
                .stream()
                .anyMatch(erTable -> StringUtil.equalsIgnoreCase(erTable.getSchema(), schemaInfo.getSchema()) && StringUtil.equalsIgnoreCase(erTable.getTable(), schemaInfo.getTable())));
        return schemaInfos.isEmpty();
    }

    /**
     * Filter out table associations with er relationships
     *
     * @param relationships
     * @param schemaInfos
     * @return result format:
     * a-b
     * c-d
     * b-c
     */
    private List<ERRelation> filterERRelation(Set<TableStat.Relationship> relationships, List<SchemaInfo> schemaInfos) {
        List<ERRelation> erRelationList = Lists.newArrayList();
        for (TableStat.Relationship relationship : relationships) {
            TableStat.Column left = relationship.getLeft();
            TableStat.Column right = relationship.getRight();
            String leftTable = left.getTable();
            String rightTable = right.getTable();
            String leftSchema = null;
            String rightSchema = null;
            for (SchemaInfo schemaInfo : schemaInfos) {
                if (schemaInfo.getTableAlias().equalsIgnoreCase(leftTable)) {
                    leftSchema = schemaInfo.getSchema();
                    leftTable = schemaInfo.getTable();
                } else if (schemaInfo.getTableAlias().equalsIgnoreCase(rightTable)) {
                    rightSchema = schemaInfo.getSchema();
                    rightTable = schemaInfo.getTable();
                }
            }
            ERTable leftERTable = new ERTable(leftSchema, leftTable, left.getName());
            ERTable rightERTable = new ERTable(rightSchema, rightTable, right.getName());
            Set<ERTable> erList = OBsharding_DServer.getInstance().getConfig().getErRelations().get(leftERTable);
            if (erList != null && !erList.isEmpty()) {
                boolean contains = erList.contains(rightERTable);
                if (contains) {
                    int compare = leftERTable.compareTo(rightERTable);
                    erRelationList.add(new ERRelation(compare < 0 ? leftERTable : rightERTable, compare < 0 ? rightERTable : leftERTable));
                }
            }
        }
        return erRelationList;
    }


    private static boolean columnInExpr(SQLExpr sqlExpr, String colName) throws SQLNonTransientException {
        String column;
        if (sqlExpr instanceof SQLIdentifierExpr) {
            column = StringUtil.removeBackQuote(((SQLIdentifierExpr) sqlExpr).getName()).toUpperCase();
        } else if (sqlExpr instanceof SQLPropertyExpr) {
            column = StringUtil.removeBackQuote(((SQLPropertyExpr) sqlExpr).getName()).toUpperCase();
        } else {
            throw new SQLNonTransientException("Unhandled SQL AST node type encountered: " + sqlExpr.getClass());
        }

        return column.equals(colName.toUpperCase());
    }

    /*
     * isSubQueryClause
     * IN (select...), ANY, EXISTS, ALL , IN (1,2,3...)
     */
    private static boolean isSubQueryClause(SQLExpr sqlExpr) throws SQLNonTransientException {
        return (sqlExpr instanceof SQLInSubQueryExpr || sqlExpr instanceof SQLAnyExpr || sqlExpr instanceof SQLAllExpr ||
                sqlExpr instanceof SQLQueryExpr || sqlExpr instanceof SQLExistsExpr);
    }

    /**
     * check shardColCanBeUpdated
     * o the partition col is in OR/XOR filter,it will Failed.
     * eg :update mytab set ptn_col = val, col1 = val1 where col1 = val11 or ptn_col = val；
     * o if the set statement has the same value with the where condition,and we can router to some node
     * eg1:update mytab set ptn_col = val, col1 = val1 where ptn_col = val and (col1 = val11 or col2 = val2);
     * eg2 :update mytab set ptn_col = val, col1 = val1 where ptn_col = val and col1 = val11 and col2 = val2;
     * o update the partition column but partition column is not not in where filter. Failed
     * eg:update mytab set ptn_col = val, col1 = val1 where col1 = val11 and col2 = val22;
     * o the other operator, like between,not, Just Failed.
     *
     * @param whereClauseExpr whereClauseExpr
     * @param column          column
     * @param value           value
     * @param hasOR           the parent of whereClauseExpr hasOR/XOR
     * @return true Passed, false Failed
     */
    private boolean shardColCanBeUpdated(SQLExpr whereClauseExpr, String column, SQLExpr value, boolean hasOR)
            throws SQLNonTransientException {
        boolean canUpdate = false;
        boolean parentHasOR = false;

        if (whereClauseExpr == null)
            return false;

        if (whereClauseExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr nodeOpExpr = (SQLBinaryOpExpr) whereClauseExpr;
            /*
             * partition column exists in or/xor expr
             */
            if ((nodeOpExpr.getOperator() == SQLBinaryOperator.BooleanOr) ||
                    (nodeOpExpr.getOperator() == SQLBinaryOperator.BooleanXor)) {
                parentHasOR = true;
            }
            if (nodeOpExpr.getOperator() == SQLBinaryOperator.Equality) {
                boolean foundCol;
                SQLExpr leftExpr = nodeOpExpr.getLeft();
                SQLExpr rightExpr = nodeOpExpr.getRight();

                foundCol = columnInExpr(leftExpr, column);

                // col is partition column, 1.check it is in OR expr or not
                // 2.check the value is the same to update set expr
                if (foundCol) {
                    if (rightExpr.getClass() != value.getClass()) {
                        throw new SQLNonTransientException("SQL AST nodes type mismatch!");
                    }

                    canUpdate = rightExpr.toString().equals(value.toString()) && (!hasOR) && (!parentHasOR);
                }
            } else if (nodeOpExpr.getOperator().isLogical()) {
                if (nodeOpExpr.getLeft() != null) {
                    if (nodeOpExpr.getLeft() instanceof SQLBinaryOpExpr) {
                        canUpdate = shardColCanBeUpdated(nodeOpExpr.getLeft(), column, value, parentHasOR);
                    }
                    // else  !=,>,< between X and Y ,NOT ,just Failed
                }
                if ((!canUpdate) && nodeOpExpr.getRight() != null) {
                    if (nodeOpExpr.getRight() instanceof SQLBinaryOpExpr) {
                        canUpdate = shardColCanBeUpdated(nodeOpExpr.getRight(), column, value, parentHasOR);
                    }
                    // else  !=,>,< between X and Y ,NOT ,just Failed
                }
            } else if (isSubQueryClause(nodeOpExpr)) {
                // subQuery ,just Failed
                return false;
            }
            // else other expr,just Failed
        }
        // else  single condition but is not = ,just Failed
        return canUpdate;
    }

    private void confirmColumnNotUpdated(SQLUpdateStatement update, BaseTableConfig tableConfig, RouteResultset rrs) throws SQLNonTransientException {
        List<SQLUpdateSetItem> updateSetItem = update.getItems();
        if (updateSetItem != null && updateSetItem.size() > 0) {
            String shardingColumn = null;
            String incrementColumn = null;
            String joinColumn = null;
            if (tableConfig instanceof ShardingTableConfig) {
                shardingColumn = ((ShardingTableConfig) tableConfig).getShardingColumn();
                incrementColumn = ((ShardingTableConfig) tableConfig).getIncrementColumn();
            } else if (tableConfig instanceof ChildTableConfig) {
                joinColumn = ((ChildTableConfig) tableConfig).getJoinColumn();
                incrementColumn = ((ChildTableConfig) tableConfig).getIncrementColumn();
            }
            for (SQLUpdateSetItem item : updateSetItem) {
                String column = StringUtil.removeBackQuote(item.getColumn().toString().toUpperCase());
                //the alias must belong to sharding table because we only support update single table
                if (column.contains(StringUtil.TABLE_COLUMN_SEPARATOR)) {
                    column = column.substring(column.indexOf(".") + 1).trim().toUpperCase();
                }
                if (shardingColumn != null && shardingColumn.equals(column)) {
                    boolean canUpdate;
                    canUpdate = ((update.getWhere() != null) && shardColCanBeUpdated(update.getWhere(),
                            shardingColumn, item.getValue(), false));

                    if (!canUpdate) {
                        String msg = "Sharding column can't be updated " + tableConfig.getName() + "->" + shardingColumn;
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
                if (incrementColumn != null && column.equals(incrementColumn)) {
                    String msg = "Increment column can't be updated " + tableConfig.getName() + "->" + incrementColumn;
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
                if (joinColumn != null) {
                    if (column.equals(joinColumn)) {
                        String msg = "Parent relevant column can't be updated " + tableConfig.getName() + "->" + joinColumn;
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                    rrs.setSqlRouteCacheAble(true);
                }
            }
        }
    }

    private void confirmChildColumnNotUpdated(SQLUpdateStatement update, SchemaConfig schema, String tableName) throws SQLNonTransientException {
        if (schema.getFkErRelations() == null) {
            return;
        }
        List<SQLUpdateSetItem> updateSetItem = update.getItems();
        if (updateSetItem != null && updateSetItem.size() > 0) {
            for (SQLUpdateSetItem item : updateSetItem) {
                String column = StringUtil.removeBackQuote(item.getColumn().toString().toUpperCase());
                if (isJoinColumn(column, schema, tableName)) {
                    String msg = "child relevant column can't be updated " + tableName + "->" + column;
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        }
    }

    private boolean isJoinColumn(String column, SchemaConfig schema, String tableName) {
        Map<ERTable, Set<ERTable>> map = schema.getFkErRelations();
        ERTable key = new ERTable(schema.getName(), tableName, column);
        return map.containsKey(key);
    }

    @Override
    int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException {
        return 0;
    }

    @Override
    String getErrorMsg() {
        return MODIFY_SQL_NOT_SUPPORT_MESSAGE;
    }

    @Override
    SQLSelect acceptVisitor(SQLObject stmt, ServerSchemaStatVisitor visitor) {
        stmt.accept(visitor);
        return (SQLSelect) stmt;
    }


}
