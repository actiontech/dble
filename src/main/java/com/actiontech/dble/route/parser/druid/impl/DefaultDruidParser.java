/*
 * Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.mpp.IsValue;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
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
    protected DruidShardingParseInfo ctx;


    public DefaultDruidParser() {
        ctx = new DruidShardingParseInfo();
    }

    /**
     * @param schema
     * @param stmt
     * @param sc
     */
    public SchemaConfig parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor schemaStatVisitor, ServerConnection sc) throws SQLException {
        ctx = new DruidShardingParseInfo();
        schema = visitorParse(schema, rrs, stmt, schemaStatVisitor, sc);
        changeSql(schema, rrs, stmt, cachePool);
        return schema;
    }


    @Override
    public void changeSql(SchemaConfig schema, RouteResultset rrs,
                          SQLStatement stmt, LayerCachePool cachePool) throws SQLException {

    }

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        stmt.accept(visitor);
        if (visitor.getNotSupportMsg() != null) {
            throw new SQLNonTransientException(visitor.getNotSupportMsg());
        }
        Map<String, String> tableAliasMap = getTableAliasMap(visitor.getAliasMap());
        ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(tableAliasMap, visitor.getConditionList()));
        return schema;
    }

    private Map<String, String> getTableAliasMap(Map<String, String> originTableAliasMap) {
        if (originTableAliasMap == null) {
            return null;
        }
        Map<String, String> tableAliasMap = new HashMap<>();
        tableAliasMap.putAll(originTableAliasMap);
        for (Map.Entry<String, String> entry : originTableAliasMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                if (key != null) {
                    key = key.toLowerCase();
                }
                if (value != null) {
                    value = value.toLowerCase();
                }
            }
            if (key != null) {
                int pos = key.indexOf(".");
                if (pos > 0) {
                    key = key.substring(pos + 1);
                }
            }
            if (value != null) {
                int pos = value.indexOf(".");
                if (pos > 0) {
                    value = value.substring(pos + 1);
                }
            }
            if (key != null && key.charAt(0) == '`') {
                key = key.substring(1, key.length() - 1);
            }
            if (value != null && value.charAt(0) == '`') {
                value = value.substring(1, value.length() - 1);
            }
            // remove database in database.table
            if (key != null) {
                boolean needAddTable = false;
                if (key.equals(value)) {
                    needAddTable = true;
                }
                if (needAddTable) {
                    ctx.addTable(key);
                }
                tableAliasMap.put(key, value);
            }
        }
        ctx.setTableAliasMap(tableAliasMap);
        return tableAliasMap;
    }

    private List<RouteCalculateUnit> buildRouteCalculateUnits(Map<String, String> tableAliasMap, List<List<Condition>> conditionList) {
        List<RouteCalculateUnit> retList = new ArrayList<>();
        //find partition column in condition
        for (List<Condition> aConditionList : conditionList) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            for (Condition condition : aConditionList) {
                List<Object> values = condition.getValues();
                if (values.size() == 0) {
                    continue;
                }
                if (checkConditionValues(values)) {
                    String columnName = StringUtil.removeBackQuote(condition.getColumn().getName().toUpperCase());
                    String tableName = StringUtil.removeBackQuote(condition.getColumn().getTable());
                    if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                        tableName = tableName.toLowerCase();
                    }
                    if (tableAliasMap != null && tableAliasMap.get(tableName) == null) {
                        //ignore subQuery's alias
                        continue;
                    }
                    if (tableAliasMap != null && tableAliasMap.get(tableName) != null &&
                            !tableAliasMap.get(tableName).equals(tableName)) {
                        tableName = tableAliasMap.get(tableName);
                    }
                    String operator = condition.getOperator();

                    //execute only between ,in and =
                    if (operator.equals("between")) {
                        RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
                        routeCalculateUnit.addShardingExpr(tableName, columnName, rv);
                    } else if (operator.equals("=") || operator.toLowerCase().equals("in")) {
                        routeCalculateUnit.addShardingExpr(tableName, columnName, values.toArray());
                    } else if (operator.equals("IS")) {
                        IsValue isValue = new IsValue(values.toArray());
                        routeCalculateUnit.addShardingExpr(tableName, columnName, isValue);
                    }
                }
            }
            retList.add(routeCalculateUnit);
        }
        return retList;
    }

    private boolean checkConditionValues(List<Object> values) {
        for (Object value : values) {
            if (value != null && !value.toString().equals("")) {
                return true;
            }
        }
        return false;
    }

    public DruidShardingParseInfo getCtx() {
        return ctx;
    }


    void checkTableExists(TableConfig tc, String schemaName, String tableName, ServerPrivileges.CheckType chekcType) throws SQLException {
        if (tc == null) {
            if (DbleServer.getInstance().getTmManager().getSyncView(schemaName, tableName) != null) {
                String msg = "View '" + schemaName + "." + tableName + "' Not Support " + chekcType;
                throw new SQLException(msg, "HY000", ErrorCode.ERR_NOT_SUPPORTED);
            }
            String msg = "Table '" + schemaName + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        } else {
            //it is strict
            if (DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaName, tableName) == null) {
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
        statement = RouterUtil.removeSchema(statement, schema.getName());
        rrs.setStatement(statement);
        String dataNodeTarget = dataNode.get();
        if (dataNodeTarget == null) {
            //no_name node
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
}
