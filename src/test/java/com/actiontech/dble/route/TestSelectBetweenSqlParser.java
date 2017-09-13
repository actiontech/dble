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

/**
 * @author lxy
 */
@Ignore
public class TestSelectBetweenSqlParser {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();

    public TestSelectBetweenSqlParser() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
    }

    @Test
    public void testBetweenSqlRoute() throws SQLException {
        String sql = "select * from offer_detail where offer_id between 1 and 33";
        SchemaConfig schema = schemaMap.get("cndb");
        RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(5, rrs.getNodes().length);

        sql = "select * from offer_detail where col_1 = 33 and offer_id between 1 and 33 and col_2 = 18";
        schema = schemaMap.get("cndb");
        rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(5, rrs.getNodes().length);

        //		sql = "select b.* from offer_date b join  offer_detail a on a.id=b.id " +
        //				"where b.col_date between '2014-02-02' and '2014-04-12' and col_1 = 3 and offer_id between 1 and 33";


        sql = "select b.* from offer_detail a  join  offer_date b on a.id=b.id " +
                "where b.col_date between '2014-02-02' and '2014-04-12' and col_1 = 3 and offer_id between 1 and 33";
        //		sql = "select a.* from offer_detail a join offer_date b on a.id=b.id " +
        //				"where b.col_date = '2014-04-02' and col_1 = 33 and offer_id =1";
        schema = schemaMap.get("cndb");
        rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);

        sql = "select b.* from  offer_date b " +
                "where b.col_date > '2014-02-02'";
        //		sql = "select a.* from offer_detail a join offer_date b on a.id=b.id " +
        //				"where b.col_date = '2014-04-02' and col_1 = 33 and offer_id =1";
        schema = schemaMap.get("cndb");
        rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(128, rrs.getNodes().length);

        sql = "select * from offer_date where col_1 = 33 and col_date between '2014-01-02' and '2014-01-12'";
        schema = schemaMap.get("cndb");
        rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);

        sql = "select * from offer_date a where col_1 = 33 and a.col_date between '2014-01-02' and '2014-01-12'";
        schema = schemaMap.get("cndb");
        rrs = RouteStrategyFactory.getRouteStrategy().route(schema, -1, sql,
				null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
    }
}
