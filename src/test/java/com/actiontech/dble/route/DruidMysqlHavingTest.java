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
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;

@Ignore
public class DruidMysqlHavingTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

    public DruidMysqlHavingTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
        routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }

    @Test
    public void testHaving() throws SQLException {
        String sql = "select avg(offer_id) avgofferid, member_id from offer_detail group by member_id having avgofferid > 100";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select avg(offer_id) avgofferid, member_id from offer_detail group by member_id having avg(offer_id) > 100";
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having countofferid > 100";
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having count(offer_id) > 100";
        rrs = routeStrategy.route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

    }
}
