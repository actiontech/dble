/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;

import java.sql.SQLNonTransientException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;

abstract class DruidInsertReplaceParser extends DefaultDruidParser {
    static RouteResultset routeByERParentKey(RouteResultset rrs, TableConfig tc, String joinKeyVal, SchemaInfo schemaInfo)
            throws SQLNonTransientException {
        if (tc.getDirectRouteTC() != null) {
            Set<ColumnRoutePair> parentColVal = new HashSet<>(1);
            ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
            parentColVal.add(pair);
            checkDefaultValues(joinKeyVal, tc, schemaInfo.getSchema(), tc.getJoinKey());
            Set<String> dataNodeSet = RouterUtil.ruleCalculate(tc.getDirectRouteTC(), parentColVal);
            if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
                throw new SQLNonTransientException("parent key can't find  valid data node ,expect 1 but found: " + dataNodeSet.size());
            }
            String dn = dataNodeSet.iterator().next();
            if (SQLJob.LOGGER.isDebugEnabled()) {
                SQLJob.LOGGER.debug("found partion node (using parent partition rule directly) for child table to insert  " + dn + " sql :" + rrs.getStatement());
            }
            return RouterUtil.routeToSingleNode(rrs, dn);
        }
        return null;
    }


    /**
     * check if the column is not null and the
     *
     */
    static void checkDefaultValues(String columnValue, TableConfig tableConfig, String schema, String partitionColumn) throws SQLNonTransientException {

        if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
            StructureMeta.TableMeta meta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schema, tableConfig.getName());
            for (StructureMeta.ColumnMeta columnMeta : meta.getColumnsList()) {
                if (!columnMeta.getCanNull()) {
                    if (columnMeta.getName().equalsIgnoreCase(partitionColumn)) {
                        String msg = "Sharding column can't be null when the table in MySQL column is not null";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
            }
        }
    }

    static String shardingValueToSting(SQLExpr valueExpr) throws SQLNonTransientException {
        String shardingValue = null;
        if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) valueExpr;
            shardingValue = intExpr.getNumber() + "";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) valueExpr;
            shardingValue = charExpr.getText();
        }

        if (shardingValue == null && !(valueExpr instanceof SQLNullExpr)) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" + valueExpr.toString());
        }
        return shardingValue;
    }

    protected static int getIdxGlobalByMeta(boolean isGlobalCheck, StructureMeta.TableMeta orgTbMeta, StringBuilder sb, int colSize) {
        int idxGlobal = -1;
        sb.append("(");
        for (int i = 0; i < colSize; i++) {
            String column = orgTbMeta.getColumnsList().get(i).getName();
            if (i > 0) {
                sb.append(",");
            }
            sb.append(column);
            if (isGlobalCheck && column.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                idxGlobal = i; // find the index of inner column
            }
        }
        sb.append(")");
        return idxGlobal;
    }

    int getIncrementKeyIndex(SchemaInfo schemaInfo, String incrementColumn) throws SQLNonTransientException {
        if (incrementColumn == null) {
            throw new SQLNonTransientException("please make sure the primaryKey's config is not null in schemal.xml");
        }
        int primaryKeyIndex = -1;
        StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (tbMeta != null) {
            boolean hasIncrementColumn = false;
            StructureMeta.IndexMeta primaryKey = tbMeta.getPrimary();
            if (primaryKey != null) {
                for (int i = 0; i < tbMeta.getColumnsList().size(); i++) {
                    if (incrementColumn.equalsIgnoreCase(tbMeta.getColumns(i).getName())) {
                        hasIncrementColumn = true;
                        break;
                    }
                }
            }
            if (!hasIncrementColumn) {
                String msg = "please make sure your table structure has primaryKey or incrementColumn";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }

            for (int i = 0; i < tbMeta.getColumnsCount(); i++) {
                if (incrementColumn.equalsIgnoreCase(tbMeta.getColumns(i).getName())) {
                    return i;
                }
            }
        }
        return primaryKeyIndex;
    }

    int getTableColumns(SchemaInfo schemaInfo, List<SQLExpr> columnExprList)
            throws SQLNonTransientException {
        if (columnExprList == null || columnExprList.size() == 0) {
            StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta == null) {
                String msg = "Meta data of table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            return tbMeta.getColumnsCount();
        } else {
            return columnExprList.size();
        }
    }

    int getShardingColIndex(SchemaInfo schemaInfo, List<SQLExpr> columnExprList, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = -1;
        if (columnExprList == null || columnExprList.size() == 0) {
            StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta != null) {
                for (int i = 0; i < tbMeta.getColumnsCount(); i++) {
                    if (partitionColumn.equalsIgnoreCase(tbMeta.getColumns(i).getName())) {
                        return i;
                    }
                }
            }
            return shardingColIndex;
        }
        for (int i = 0; i < columnExprList.size(); i++) {
            if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackQuote(columnExprList.get(i).toString()))) {
                return i;
            }
        }
        return shardingColIndex;
    }


    void fetchChildTableToRoute(TableConfig tc, String joinKeyVal, ServerConnection sc, SchemaConfig schema, String sql, RouteResultset rrs) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            //get child result will be blocked, so use ComplexQueryExecutor
            @Override
            public void run() {
                // route by sql query root parent's data node
                String findRootTBSql = tc.getLocateRTableKeySql() + joinKeyVal;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("to find root parent's node sql :" + findRootTBSql);
                }
                FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, sc.getSession2());
                try {
                    String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
                    if (dn == null) {
                        sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "can't find (root) parent sharding node for sql:" + sql);
                        return;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partition node for child table to insert " + dn + " sql :" + sql);
                    }
                    RouterUtil.routeToSingleNode(rrs, dn);
                    sc.getSession2().execute(rrs);
                } catch (ConnectionException e) {
                    sc.setTxInterrupt(e.toString());
                    sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.toString());
                }
            }
        });
    }
}
