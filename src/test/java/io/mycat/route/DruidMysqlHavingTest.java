package io.mycat.route;

import java.sql.SQLException;
import java.util.Map;

import org.junit.Test;
import org.junit.Ignore;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import junit.framework.Assert;

@Ignore
public class DruidMysqlHavingTest
{
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
        RouteResultset rrs = routeStrategy.route(schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

		sql = "select avg(offer_id) avgofferid, member_id from offer_detail group by member_id having avg(offer_id) > 100";
        rrs = routeStrategy.route(schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having countofferid > 100";
        rrs = routeStrategy.route(schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

        sql = "select count(offer_id) countofferid, member_id from offer_detail group by member_id having count(offer_id) > 100";
        rrs = routeStrategy.route(schema, -1, sql, null,
                null, cachePool);
        Assert.assertEquals(3, rrs.getSqlMerge().getHavingColsName().length);

	}
}
