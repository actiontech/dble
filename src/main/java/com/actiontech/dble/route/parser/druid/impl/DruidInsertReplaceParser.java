/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.handler.ExplainHandler;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.HexFormatUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;

abstract class DruidInsertReplaceParser extends DruidModifyParser {

    static final String MODIFY_SQL_NOT_SUPPORT_MESSAGE = "This `INSERT ... SELECT Syntax` is not supported!";

    /**
     * The insert/replace .... select route function interface
     * check the insert table config and category discussion with
     * + single node table
     * + sharding table
     * + multi-node global table
     *
     * @param service
     * @param rrs
     * @param stmt
     * @param visitor
     * @param schemaInfo
     * @throws SQLException
     */
    protected void tryRouteInsertQuery(ShardingService service, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, SchemaUtil.SchemaInfo schemaInfo) throws SQLException {
        // insert into .... select ....
        SQLSelect select = acceptVisitor(stmt, visitor);

        String tableName = schemaInfo.getTable();
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        BaseTableConfig tc = schema.getTables().get(tableName);

        Collection<String> routeShardingNodes;


        for (String selectTable : visitor.getSelectTableList()) {
            SchemaUtil.SchemaInfo schemaInfox = SchemaUtil.getSchemaInfo(service.getUser(), schema, selectTable);
            if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfox.getSchema(), schemaInfox.getTable(), ShardingPrivileges.CheckType.SELECT)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfox.getSchema()));
        }
        rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));


        boolean isGlobal = false;
        if (tc == null || tc instanceof SingleTableConfig) {
            //only require when all the table and the route condition route to same node
            Map<String, String> tableAliasMap = getTableAliasMap(schema.getName(), visitor.getAliasMap());
            ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(visitor.getAllWhereUnit(), tableAliasMap, schema.getName()));
            checkForSingleNodeTable(visitor, tc == null ? schema.getShardingNode() : tc.getShardingNodes().get(0), rrs, service.getCharset().getClient());
            routeShardingNodes = ImmutableList.of(tc == null ? schema.getShardingNode() : tc.getShardingNodes().get(0));
            //RouterUtil.routeToSingleNode(rrs, tc == null ? schema.getShardingNode() : tc.getShardingNodes().get(0));
        } else if (tc instanceof GlobalTableConfig) {
            isGlobal = true;
            routeShardingNodes = checkForMultiNodeGlobal(service.getUser(), visitor, (GlobalTableConfig) tc, schema);
        } else if (tc instanceof ShardingTableConfig) {
            routeShardingNodes = checkForShardingTable(visitor, select, service, rrs, (ShardingTableConfig) tc, schemaInfo, stmt, schema);
        } else {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }

        //finally route for the result
        RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, isGlobal);

        String sql = rrs.getStatement();
        for (Pair<String, String> table : ctx.getTables()) {
            String schemaName = table.getKey();
            sql = RouterUtil.removeSchema(sql, schemaName);
        }
        rrs.setStatement(sql);

        rrs.setFinishedRoute(true);
    }

    static String shardingValueToSting(SQLExpr valueExpr, String clientCharset, String dataType) throws SQLNonTransientException {
        String shardingValue = null;
        if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr) valueExpr;
            shardingValue = intExpr.getNumber() + "";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr) valueExpr;
            shardingValue = charExpr.getText();
        } else if (valueExpr instanceof SQLHexExpr) {
            SQLHexExpr hexExpr = (SQLHexExpr) valueExpr;
            if (FieldUtil.isNumberType(dataType)) {
                shardingValue = Long.parseLong(hexExpr.getHex(), 16) + "";
            } else {
                shardingValue = HexFormatUtil.fromHex(hexExpr.getHex(), CharsetUtil.getJavaCharset(clientCharset));
            }
        }

        if (shardingValue == null && !(valueExpr instanceof SQLNullExpr)) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" + valueExpr.toString());
        }
        return shardingValue;
    }

    int getIncrementKeyIndex(SchemaInfo schemaInfo, String incrementColumn) throws SQLNonTransientException {
        if (incrementColumn == null) {
            throw new SQLNonTransientException("please make sure the incrementColumn's config is not null in schemal.xml");
        }
        TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (tbMeta != null) {
            for (int i = 0; i < tbMeta.getColumns().size(); i++) {
                if (incrementColumn.equalsIgnoreCase(tbMeta.getColumns().get(i).getName())) {
                    return i;
                }
            }
            String msg = "please make sure your table structure has incrementColumn";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        return -1;
    }

    int getTableColumns(SchemaInfo schemaInfo, List<SQLExpr> columnExprList)
            throws SQLNonTransientException {
        if (columnExprList == null || columnExprList.size() == 0) {
            TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta == null) {
                String msg = "Meta data of table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            return tbMeta.getColumns().size();
        } else {
            return columnExprList.size();
        }
    }

    int getShardingColIndex(SchemaInfo schemaInfo, List<SQLExpr> columnExprList, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = -1;
        if (columnExprList == null || columnExprList.size() == 0) {
            TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta != null) {
                for (int i = 0; i < tbMeta.getColumns().size(); i++) {
                    if (partitionColumn.equalsIgnoreCase(tbMeta.getColumns().get(i).getName())) {
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


    void fetchChildTableToRoute(ChildTableConfig tc, String joinColumnVal, ShardingService service, SchemaConfig schema, String sql, RouteResultset rrs, boolean isExplain) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            //get child result will be blocked, so use ComplexQueryExecutor
            @Override
            public void run() {
                // route by sql query root parent's shardingNode
                String findRootTBSql = tc.getLocateRTableKeySql() + joinColumnVal;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("to find root parent's node sql :" + findRootTBSql);
                }
                FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, service.getSession2());
                try {
                    String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getShardingNodes());
                    if (dn == null) {
                        service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "can't find (root) parent sharding node for sql:" + sql);
                        return;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partition node for child table to insert " + dn + " sql :" + sql);
                    }
                    RouterUtil.routeToSingleNode(rrs, dn);
                    if (isExplain) {
                        ExplainHandler.writeOutHeadAndEof(service, rrs);
                    } else {
                        service.getSession2().execute(rrs);
                    }
                } catch (ConnectionException e) {
                    service.setTxInterrupt(e.toString());
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.toString());
                }
            }
        });
    }

    @Override
    String getErrorMsg() {
        return MODIFY_SQL_NOT_SUPPORT_MESSAGE;
    }
}
