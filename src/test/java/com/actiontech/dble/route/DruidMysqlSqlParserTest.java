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
import com.actiontech.dble.server.parser.ServerParse;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;

@Ignore
public class DruidMysqlSqlParserTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

    public DruidMysqlSqlParserTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
        routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Test
    public void testLimitPage() throws SQLException {
        String sql = "select * from offer order by id desc limit 5,10";
        SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());
        Assert.assertEquals("dn2", rrs.getNodes()[1].getName());

        sql = rrs.getNodes()[0].getStatement();
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(15, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(15, rrs.getLimitSize());

        sql = "select * from offer1 order by id desc limit 5,10";
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(5, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(5, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());


        sql = "select * from offer1 order by id desc limit 10";
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);
        Assert.assertEquals(0, rrs.getLimitStart());
        Assert.assertEquals(10, rrs.getLimitSize());
        Assert.assertEquals(0, rrs.getNodes()[0].getLimitStart());
        Assert.assertEquals(10, rrs.getNodes()[0].getLimitSize());
        Assert.assertEquals("dn1", rrs.getNodes()[0].getName());

    }

    @Test
    public void testLockTableSql() throws SQLException {
        String sql = "lock tables goods write";
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = routeStrategy.route(schema, ServerParse.LOCK, sql, null, cachePool);
        Assert.assertEquals(3, rrs.getNodes().length);
    }


}
