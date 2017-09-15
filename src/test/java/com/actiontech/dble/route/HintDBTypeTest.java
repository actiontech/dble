/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.cache.CacheService;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.parser.ServerParse;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

@Ignore
public class HintDBTypeTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

    public HintDBTypeTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
        routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    /**
     * testHint
     *
     * @throws Exception
     */
    @Test
    public void testHint() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        //(new hint,/*!dble*/),runOnSlave=false force master
        String sql = "/*!dble:db_type=master*/select * from employee where sharding_id=1";
        CacheService cacheService = new CacheService(false);
        RouteService routerService = new RouteService(cacheService);
        RouteResultset rrs = routerService.route(schema, ServerParse.SELECT, sql, null);
        Assert.assertTrue(!rrs.getRunOnSlave());

        //(new hint,/*#dble*/),runOnSlave=false force master
        sql = "/*#dble:db_type=master*/select * from employee where sharding_id=1";
        rrs = routerService.route(schema, ServerParse.SELECT, sql, null);
        Assert.assertTrue(!rrs.getRunOnSlave());

        //(new hint,/*dble*/),runOnSlave=false force master
        sql = "/*dble:db_type=master*/select * from employee where sharding_id=1";
        rrs = routerService.route(schema, ServerParse.SELECT, sql, null);
        Assert.assertTrue(!rrs.getRunOnSlave());

        //no hint ,runOnSlave=null
        sql = "select * from employee where sharding_id=1";
        rrs = routerService.route(schema, ServerParse.SELECT, sql, null);
        Assert.assertTrue(rrs.getRunOnSlave() == null);
    }
}
