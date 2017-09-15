/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.cache.CacheService;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.route.parser.druid.impl.ddl.DruidCreateTableParser;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Ignore
public class DDLRouteTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;

    public DDLRouteTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        //SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        //schemaMap = schemaLoader.getSchemas();
        //RouteStrategyFactory.init();
        //routeStrategy = RouteStrategyFactory.getRouteStrategy();
    }


    @Test
    public void testSpecialCharDDL() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        CacheService cacheService = new CacheService(false);
        RouteService routerService = new RouteService(cacheService);

        // alter table test
        String sql = " ALTER TABLE COMPANY\r\nADD COLUMN TEST  VARCHAR(255) NULL AFTER CREATE_DATE,\r\n CHARACTER SET = UTF8";
        sql = RouterUtil.getFixedSql(sql);
        List<String> dataNodes = new ArrayList<>();
        String tablename = "COMPANY";
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);
    }


    /**
     * ddl deal test
     *
     * @throws Exception
     */
    @Test
    public void testDDL() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        CacheService cacheService = new CacheService(false);
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();

        //TODO : modify by zhuam
        String tablename = "company";
        tablename = tablename.toUpperCase();

        List<String> dataNodes = new ArrayList<>();
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();

        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }

    @Test
    public void testCreateTableForbidden() throws Exception {
        Method[] fa = DruidCreateTableParser.class.getDeclaredMethods();
        SQLCreateTableStatement[] testSQLStatement = new SQLCreateTableStatement[100];
        InvocationTargetException[] result = new InvocationTargetException[100];
        String[] createSqls = {
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT)",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) CHARACTER SET = 'utf8'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) COMMENT = '一二三四五'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENCRYPTION  = 'Y'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) KEY_BLOCK_SIZE  = 1",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ROW_FORMAT  = 'BLOB'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) STATS_AUTO_RECALC   = 0",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) STATS_PERSISTENT = 0",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) STATS_SAMPLE_PAGES   = 5",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'InnoDB'",

                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'MyISAM'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'MEMORY'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'CSV'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'ARCHIVE'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'EXAMPLE'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'HEAP'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'MERGE'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'NDB'",

                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) DATA DIRECTORY = '/data'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) AUTO_INCREMENT = 1",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) AUTO_INCREMENT = 2",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) PARTITION BY HASH(XX)",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT)PARTITION BY HASH ( YEAR(XX) )",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) PARTITION BY KEY(XX) PARTITIONS 4"

        };
        //test sharding status
        for (int i = 0; i < createSqls.length; i++) {
            SQLStatementParser parser = new MySqlStatementParser(createSqls[i]);
            testSQLStatement[i] = (SQLCreateTableStatement) parser.parseStatement();
        }

        for (Method f : fa) {
            if (f.getName().equals("sharingTableCheck")) {
                f.setAccessible(true);
                for (int i = 0; i < createSqls.length; i++) {
                    try {
                        DruidCreateTableParser createTableParser = new DruidCreateTableParser();
                        Object[] prams = {testSQLStatement[i]};
                        try {
                            f.invoke(createTableParser, prams);
                        } catch (InvocationTargetException e) {
                            result[i] = e;
                            continue;
                        }
                        result[i] = null;
                    } catch (Exception e) {
                        System.out.print("there is something wrong with the Parser" + e.getMessage());
                    }
                }
            }
        }

        Assert.assertTrue(result[0] == null);
        Assert.assertTrue(result[1] == null);
        Assert.assertTrue(result[2] == null);
        Assert.assertTrue(result[3] == null);
        Assert.assertTrue(result[4] == null);
        Assert.assertTrue(result[5] == null);
        Assert.assertTrue(result[6] == null);
        Assert.assertTrue(result[7] == null);
        Assert.assertTrue(result[8] == null);
        Assert.assertTrue(result[9] == null);

        Assert.assertTrue(result[10].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[11].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[12].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[13].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[14].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[15].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[16].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[17].getTargetException().getMessage().contains("ENGINE InnoDB"));

        Assert.assertTrue(result[18].getTargetException().getMessage().contains("DATA DIRECTORY"));
        Assert.assertTrue(result[19].getTargetException().getMessage().contains("AUTO_INCREMENT"));
        Assert.assertTrue(result[20].getTargetException().getMessage().contains("AUTO_INCREMENT"));
        Assert.assertTrue(result[21].getTargetException().getMessage().contains("Partition"));
        Assert.assertTrue(result[22].getTargetException().getMessage().contains("Partition"));
        Assert.assertTrue(result[23].getTargetException().getMessage().contains("Partition"));
    }


    @Test
    public void testDDLDefaultNode() throws Exception {
        SchemaConfig schema = schemaMap.get("solo1");
        CacheService cacheService = new CacheService(false);
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();

        String tablename = "COMPANY";
        tablename = tablename.toUpperCase();

        List<String> dataNodes = new ArrayList<>();
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();
        if (nodeSize == 0 && schema.getDataNode() != null) {
            nodeSize = 1;
        }

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize == 0 && schema.getDataNode() != null) {
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table if exists COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize == 0 && schema.getDataNode() != null) {
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize == 0 && schema.getDataNode() != null) {
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename = "COMPANY";
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize == 0 && schema.getDataNode() != null) {
            nodeSize = 1;
        }

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(schema, sqlType, sql, null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }


    @Test
    public void testTableMetaRead() throws Exception {
        final SchemaConfig schema = schemaMap.get("cndb");

        String sql = "desc offer";
        RouteResultset rrs = routeStrategy.route(schema, ServerParse.DESCRIBE, sql, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer";
        rrs = routeStrategy.route(schema, ServerParse.DESCRIBE, sql, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer col1";
        rrs = routeStrategy.route(schema, ServerParse.DESCRIBE, sql, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer col1", rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  offer  IN db_name WHERE true";
        rrs = routeStrategy.route(schema, ServerParse.SHOW, sql, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  db.offer  IN db_name WHERE true";
        rrs = routeStrategy.route(schema, ServerParse.SHOW, sql, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());


        sql = "SHOW FULL TABLES FROM `TESTDB` WHERE Table_type != 'VIEW'";
        rrs = routeStrategy.route(schema, ServerParse.SHOW, sql, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("SHOW FULL TABLES WHERE Table_type != 'VIEW'", rrs.getNodes()[0].getStatement());

        sql = "SHOW INDEX  IN offer FROM  db_name";
        rrs = routeStrategy.route(schema, ServerParse.SHOW, sql, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW INDEX  FROM offer",
                rrs.getNodes()[0].getStatement());
    }

}
