/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.perf;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.loader.SchemaLoader;
import com.actiontech.dble.config.loader.xml.XMLSchemaLoader;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.factory.RouteStrategyFactory;

import java.sql.SQLException;

/**
 * @author mycat
 */
public class NoShardingSpace {
    private SchemaConfig schema;
    private static int total = 1000000;

    public NoShardingSpace() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile, true, null);
        schema = schemaLoader.getSchemas().get("dubbo");
    }

    public void testDefaultSpace() throws SQLException {
        SchemaConfig schema = this.schema;
        String stmt = "insert into offer (member_id, gmt_create) values ('1','2001-09-13 20:20:33')";
        for (int i = 0; i < total; i++) {
            RouteStrategyFactory.getRouteStrategy().route(schema, -1, stmt, null);
        }
    }

    public static void main(String[] args) throws SQLException {
        NoShardingSpace test = new NoShardingSpace();
        System.currentTimeMillis();

        long start = System.currentTimeMillis();
        test.testDefaultSpace();
        long end = System.currentTimeMillis();
        System.out.println("take " + (end - start) + " ms. avg " + (end - start + 0.0) / total);
    }
}