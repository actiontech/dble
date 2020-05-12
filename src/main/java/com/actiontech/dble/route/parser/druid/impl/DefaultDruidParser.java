/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.CacheService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * DefaultDruidParser
 *
 * @author wang.dw
 */
public class DefaultDruidParser implements DruidParser {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);
    DruidShardingParseInfo ctx;


    public DefaultDruidParser() {
        ctx = new DruidShardingParseInfo();
    }

    public SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc, boolean isExplain) throws SQLException {
        ctx = new DruidShardingParseInfo();
        schema = visitorParse(schema, rrs, stmt, schemaStatVisitor, sc, isExplain);
        changeSql(schema, rrs, stmt, cachePool);
        return schema;
    }

    public SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc) throws SQLException {
        return this.parser(schema, rrs, stmt, originSql, cachePool, schemaStatVisitor, sc, false);
    }


    @Override
    public void changeSql(SchemaConfig schema, RouteResultset rrs,
                          SQLStatement stmt, LayerCachePool cachePool) throws SQLException {

    }

    @Override
    public SchemaConfig visitorParse(SchemaConfig schemaConfig, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        stmt.accept(visitor);
        if (visitor.getNotSupportMsg() != null) {
            throw new SQLNonTransientException(visitor.getNotSupportMsg());
        }
        String schemaName = null;
        if (schemaConfig != null) {
            schemaName = schemaConfig.getName();
        }
        Map<String, String> tableAliasMap = getTableAliasMap(schemaName, visitor.getAliasMap());
        ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(visitor.getAllWhereUnit(), tableAliasMap, schemaName));

        return schemaConfig;
    }

    private Map<String, String> getTableAliasMap(String defaultSchemaName, Map<String, String> originTableAliasMap) {
        if (originTableAliasMap == null) {
            return null;
        }

        Map<String, String> tableAliasMap = new HashMap<>(originTableAliasMap);
        for (Map.Entry<String, String> entry : originTableAliasMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            // fixme: not strict
            if (key != null && key.startsWith("`")) {
                tableAliasMap.put(key.replaceAll("`", ""), value);
            }
        }

        Iterator<Map.Entry<String, String>> iterator = tableAliasMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            String keySchemaName = defaultSchemaName;
            String valueSchemaName = defaultSchemaName;
            String key = next.getKey();
            String value = next.getValue();
            if ("subquery".equalsIgnoreCase(value)) {
                iterator.remove();
                continue;
            }
            if (key != null) {
                int pos = key.indexOf(".");
                if (pos > 0) {
                    keySchemaName = key.substring(0, pos);
                    key = key.substring(pos + 1);
                }
            }
            if (value != null) {
                int pos = value.indexOf(".");
                if (pos > 0) {
                    valueSchemaName = value.substring(0, pos);
                    value = value.substring(pos + 1);
                }
            }
            if (key != null && keySchemaName != null) {
                keySchemaName = StringUtil.removeBackQuote(keySchemaName);
                key = StringUtil.removeBackQuote(key);
                // remove database in database.table
                if (key.equals(value) && keySchemaName.equals(valueSchemaName)) {
                    Pair<String, String> tmpTable = new Pair<>(keySchemaName, key);
                    if (!ctx.getTables().contains(tmpTable)) {
                        ctx.addTable(tmpTable);
                    }
                }
            }
        }
        ctx.setTableAliasMap(tableAliasMap);
        return tableAliasMap;
    }

    public DruidShardingParseInfo getCtx() {
        return ctx;
    }


    void checkTableExists(TableConfig tc, String schemaName, String tableName, ServerPrivileges.CheckType chekcType) throws SQLException {
        if (tc == null) {
            if (ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, tableName) != null) {
                String msg = "View '" + schemaName + "." + tableName + "' Not Support " + chekcType;
                throw new SQLException(msg, "HY000", ErrorCode.ERR_NOT_SUPPORTED);
            }
            String msg = "Table '" + schemaName + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        } else {
            //it is strict
            if (ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaName, tableName) == null) {
                String msg = "Table meta '" + schemaName + "." + tableName + "' is lost,PLEASE reload @@metadata";
                LOGGER.warn(msg);
                throw new SQLException(msg, "HY000", ErrorCode.ERR_HANDLE_DATA);
            }
        }
    }

    SchemaConfig routeToNoSharding(SchemaConfig schema, RouteResultset rrs, Set<String> schemas, StringPtr dataNode) {
        String statement = rrs.getStatement();
        for (String realSchema : schemas) {
            statement = RouterUtil.removeSchema(statement, realSchema);
        }
        if (schema != null) {
            statement = RouterUtil.removeSchema(statement, schema.getName());
        }
        rrs.setStatement(statement);
        String dataNodeTarget = dataNode.get();
        if (dataNodeTarget == null) {
            //no_name node
            if (schema == null) {
                String db = SchemaUtil.getRandomDb();
                schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
            }
            dataNodeTarget = schema.getRandomDataNode();
        }
        RouterUtil.routeToSingleNode(rrs, dataNodeTarget);
        rrs.setFinishedRoute(true);
        return schema;
    }

    // avoid druid error ,default shardingSupport is true and table name like testTable_number will be parser to testTable
    //eg: testDb.testTb_1->testDb.testTb ,testDb.testTb_1_2->testDb.testTb
    String statementToString(SQLStatement statement) {
        StringBuffer buf = new StringBuffer();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(buf);
        visitor.setShardingSupport(false);
        statement.accept(visitor);
        return buf.toString();
    }


    /*
     * delete / update sharding table with limit route
     * if the update/delete with limit route to more than one sharding-table throw a new Execption
     *
     */
    void updateAndDeleteLimitRoute(RouteResultset rrs, String tableName, SchemaConfig schema) throws SQLException {
        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, unit, tableName, rrs, false, CacheService.getTableId2DataNodeCache(), null);
            if (rrsTmp != null && rrsTmp.getNodes() != null) {
                Collections.addAll(nodeSet, rrsTmp.getNodes());
            }
        }
        if (nodeSet.size() > 1) {
            throw new SQLNonTransientException("delete/update sharding table with a limit route to multiNode not support");
        } else {
            RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
            int i = 0;
            for (RouteResultsetNode aNodeSet : nodeSet) {
                nodes[i] = aNodeSet;
                i++;
            }
            rrs.setNodes(nodes);
            rrs.setFinishedRoute(true);
        }
    }
}
