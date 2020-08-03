/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * see http://dev.mysql.com/doc/refman/5.7/en/update.html
 *
 * @author huqing.yan
 */
public class DruidUpdateParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        MySqlUpdateStatement update = (MySqlUpdateStatement) stmt;
        SQLTableSource tableSource = update.getTableSource();
        String schemaName = schema == null ? null : schema.getName();
        if (tableSource instanceof SQLJoinTableSource) {
            StringPtr noShardingNode = new StringPtr(null);
            Set<String> schemas = new HashSet<>();
            if (!SchemaUtil.isNoSharding(sc, (SQLJoinTableSource) tableSource, stmt, stmt, schemaName, schemas, noShardingNode)) {
                String msg = "UPDATE query with multiple tables is not supported, sql:" + stmt;
                throw new SQLNonTransientException(msg);
            } else {
                return routeToNoSharding(schema, rrs, schemas, noShardingNode);
            }
        } else {
            SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, (SQLExprTableSource) tableSource);
            if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), ServerPrivileges.CheckType.UPDATE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            schema = schemaInfo.getSchemaConfig();
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            super.visitorParse(schema, rrs, stmt, visitor, sc, isExplain);
            if (visitor.getSubQueryList().size() > 0) {
                StringPtr noShardingNode = new StringPtr(null);
                Set<String> schemas = new HashSet<>();
                if (!SchemaUtil.isNoSharding(sc, tableSource, stmt, stmt, schemaInfo.getSchema(), schemas, noShardingNode)) {
                    String msg = "UPDATE query with sub-query  is not supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                } else {
                    return routeToNoSharding(schema, rrs, schemas, noShardingNode);
                }
            }

            String tableName = schemaInfo.getTable();
            String noShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (noShardingNode != null) {
                RouterUtil.routeToSingleNode(rrs, noShardingNode);
                rrs.setFinishedRoute(true);
                return schema;
            }
            TableConfig tc = schema.getTables().get(tableName);
            checkTableExists(tc, schema.getName(), tableName, ServerPrivileges.CheckType.UPDATE);


            if (tc.isGlobalTable()) {
                RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), tc.isGlobalTable());
                rrs.setFinishedRoute(true);
                return schema;
            }
            String partitionColumn = tc.getPartitionColumn();
            String joinKey = tc.getJoinKey();

            confirmShardColumnNotUpdated(update, schema, tableName, partitionColumn, joinKey, rrs);

            confirmChildColumnNotUpdated(update, schema, tableName);

            if (schema.getTables().get(tableName).isGlobalTable() && ctx.getTables().size() > 1) {
                throw new SQLNonTransientException("global table is not supported in multi table related update " + tableName);
            }

            if (update.getLimit() != null) {
                this.updateAndDeleteLimitRoute(rrs, tableName, schema);
            }

            if (ctx.getTables().size() == 0) {
                ctx.addTable(new Pair<>(schema.getName(), tableName));
            }
        }
        return schema;
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
     * @param whereClauseExpr
     * @param column
     * @param value
     * @return true Passed, false Failed
     * @hasOR the parent of whereClauseExpr hasOR/XOR
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

    private void confirmShardColumnNotUpdated(SQLUpdateStatement update, SchemaConfig schema, String tableName, String partitionColumn, String joinKey, RouteResultset rrs) throws SQLNonTransientException {
        List<SQLUpdateSetItem> updateSetItem = update.getItems();
        if (updateSetItem != null && updateSetItem.size() > 0) {
            boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
            for (SQLUpdateSetItem item : updateSetItem) {
                String column = StringUtil.removeBackQuote(item.getColumn().toString().toUpperCase());
                //the alias must belong to sharding table because we only support update single table
                if (column.contains(StringUtil.TABLE_COLUMN_SEPARATOR)) {
                    column = column.substring(column.indexOf(".") + 1).trim().toUpperCase();
                }
                if (partitionColumn != null && partitionColumn.equals(column)) {
                    boolean canUpdate;
                    canUpdate = ((update.getWhere() != null) && shardColCanBeUpdated(update.getWhere(),
                            partitionColumn, item.getValue(), false));

                    if (!canUpdate) {
                        String msg = "Sharding column can't be updated " + tableName + "->" + partitionColumn;
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
                if (hasParent) {
                    if (column.equals(joinKey)) {
                        String msg = "Parent relevant column can't be updated " + tableName + "->" + joinKey;
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                    rrs.setSqlRouteCacheAble(true);
                }
            }
        }
    }


    /**
     * confirmChildColumnNotUpdated
     *
     * @throws SQLNonTransientException
     */
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


    /**
     * @param schema
     * @param tableName
     * @return
     */
    private boolean isJoinColumn(String column, SchemaConfig schema, String tableName) {
        Map<ERTable, Set<ERTable>> map = schema.getFkErRelations();
        ERTable key = new ERTable(schema.getName(), tableName, column);
        return map.containsKey(key);
    }
}
