/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.stat.TableStat.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DefaultDruidParser
 *
 * @author wang.dw
 */
public class DefaultDruidParser implements DruidParser {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);
    protected DruidShardingParseInfo ctx;

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
        List<List<Condition>> mergedConditionList = new ArrayList<>();
        if (visitor.hasOrCondition()) {
            mergedConditionList = visitor.splitConditions();
        } else {
            mergedConditionList.add(visitor.getConditions());
        }
        Map<String, String> tableAliasMap = getTableAliasMap(visitor.getAliasMap());
        ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(tableAliasMap, mergedConditionList));
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
            if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
                if (key != null) {
                    key = key.toLowerCase();
                }
                if (value != null) {
                    value = value.toLowerCase();
                }
            }
            if (key != null && key.contains("`")) {
                key = key.replaceAll("`", "");
            }
            if (value != null && value.contains("`")) {
                value = value.replaceAll("`", "");
            }
            // remove database in database.table
            if (key != null) {
                boolean needAddTable = false;
                if (key.equals(value)) {
                    needAddTable = true;
                }
                int pos = key.indexOf(".");
                if (pos > 0) {
                    key = key.substring(pos + 1);
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
                    if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
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
}
