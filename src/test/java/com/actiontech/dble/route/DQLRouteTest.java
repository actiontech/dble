/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class DQLRouteTest {

    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy();
    private Map<String, String> tableAliasMap = new HashMap<String, String>();

    protected DruidShardingParseInfo ctx;

    public DQLRouteTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
    }

    @Test
    public void test() throws Exception {
        String stmt = "select * from `offer` where id = 100";
        SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = new RouteResultset(stmt, 7);
        SQLStatementParser parser = null;

        parser = new MySqlStatementParser(stmt);
        SQLStatement statement;
        ServerSchemaStatVisitor visitor = null;

        try {
            statement = parser.parseStatement();
            visitor = new ServerSchemaStatVisitor();
        } catch (Exception t) {
            throw new SQLSyntaxErrorException(t);
        }
        ctx = new DruidShardingParseInfo();

        List<RouteCalculateUnit> taskList = visitorParse(rrs, statement, visitor);
        Assert.assertEquals(true, !taskList.get(0).getTablesAndConditions().isEmpty());
    }

    @SuppressWarnings("unchecked")
    private List<RouteCalculateUnit> visitorParse(RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor) throws Exception {

        stmt.accept(visitor);

        List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
        if (visitor.hasOrCondition()) {//contains or
            mergedConditionList = visitor.splitConditions();
        } else {
            mergedConditionList.add(visitor.getConditions());
        }

        if (visitor.getAliasMap() != null) {
            for (Map.Entry<String, String> entry : visitor.getAliasMap().entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && key.indexOf("`") >= 0) {
                    key = key.replaceAll("`", "");
                }
                if (value != null && value.indexOf("`") >= 0) {
                    value = value.replaceAll("`", "");
                }
                // remove the database of table
                if (key != null) {
                    int pos = key.indexOf(".");
                    if (pos > 0) {
                        key = key.substring(pos + 1);
                    }
                }

                if (key.equals(value)) {
                    ctx.addTable(key.toUpperCase());
                }
                // else {
                // tableAliasMap.put(key, value);
                // }
                tableAliasMap.put(key.toUpperCase(), value);
            }
            visitor.getAliasMap().putAll(tableAliasMap);
            ctx.setTableAliasMap(tableAliasMap);
        }

        Class<?> clazz = Class.forName("com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser");
        Method buildRouteCalculateUnits = clazz.getDeclaredMethod("buildRouteCalculateUnits",
                new Class[]{SchemaStatVisitor.class, List.class});
        //System.out.println("buildRouteCalculateUnits:\t" + buildRouteCalculateUnits);
        Object newInstance = clazz.newInstance();
        buildRouteCalculateUnits.setAccessible(true);
        Object returnValue = buildRouteCalculateUnits.invoke(newInstance,
                new Object[]{visitor, mergedConditionList});
        List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();
        if (returnValue instanceof ArrayList<?>) {
            retList.add(((ArrayList<RouteCalculateUnit>) returnValue).get(0));
            //retList = (ArrayList<RouteCalculateUnit>)returnValue;
            //System.out.println(taskList.get(0).getTablesAndConditions().values());
        }
        return retList;
    }

}
