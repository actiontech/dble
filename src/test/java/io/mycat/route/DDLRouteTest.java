package io.mycat.route;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.route.parser.druid.impl.ddl.DruidCreateTableParser;
import org.junit.Test;

import io.mycat.SimpleCachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteService;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.parser.ServerParse;
import junit.framework.Assert;

public class DDLRouteTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy ;

    public DDLRouteTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        //SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        //schemaMap = schemaLoader.getSchemas();
        //RouteStrategyFactory.init();
        //routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
    }


    @Test
    public void testSpecialCharDDL() throws Exception {
        SchemaConfig schema = schemaMap.get("TESTDB");
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // alter table test
        String  sql = " ALTER TABLE COMPANY\r\nADD COLUMN TEST  VARCHAR(255) NULL AFTER CREATE_DATE,\r\n CHARACTER SET = UTF8";
        sql = RouterUtil.getFixedSql(sql);
        List<String> dataNodes = new ArrayList<>();
        String  tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(sql, 0));
        Map<String, TableConfig>  tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize  = dataNodes.size();

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
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
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();

        //TODO : modify by zhuam
        // 小写表名，需要额外转为 大写 做比较
        String tablename =  RouterUtil.getTableName(sql, RouterUtil.getCreateTablePos(upsql, 0));
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
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();

        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getTruncateTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);




    }

    @Test
    public void testCreateTableForbidden() throws Exception {
        Method[] fa = DruidCreateTableParser.class.getDeclaredMethods();
        SQLCreateTableStatement[] testSQLStatement = new SQLCreateTableStatement[20];
        InvocationTargetException[] result = new InvocationTargetException[20];
        String[] createSqls = {
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT)",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'InnoDB'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) PARTITION BY HASH(XX)",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) ENGINE = 'MyISAM'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) DATA DIRECTORY = '/data'",
                "CREATE TABLE SUNTEST(XX VARCHAR(40),YY VARCHAR(40),ID INT) AUTO_INCREMENT = 1"
        };
        //测试分片的状态
        for(int i = 0;i < createSqls.length;i++){
            SQLStatementParser parser = new MySqlStatementParser(createSqls[i]);
            testSQLStatement[i] = (SQLCreateTableStatement) parser.parseStatement();
        }

        for(Method f :fa){
            if(f.getName().equals("sharingTableCheck")){
                f.setAccessible(true);
                for(int i = 0;i < createSqls.length;i++) {
                    DruidCreateTableParser createTableParser = new DruidCreateTableParser();
                    Object[] prams = {testSQLStatement[i]};
                    try {
                        f.invoke(createTableParser, prams);
                    } catch (InvocationTargetException e) {
                        result[i] = e;
                        continue;
                    }
                    result[i] = null;
                }
            }
        }

        Assert.assertTrue(result[0] == null);
        Assert.assertTrue(result[1] == null);
        Assert.assertTrue(result[2].getTargetException().getMessage().contains("Partition"));
        Assert.assertTrue(result[3].getTargetException().getMessage().contains("ENGINE InnoDB"));
        Assert.assertTrue(result[4].getTargetException().getMessage().contains("DATA DIRECTORY"));
        Assert.assertTrue(result[5].getTargetException().getMessage().contains("AUTO_INCREMENT"));
    }



    @Test
    public void testDDLDefaultNode() throws Exception {
        SchemaConfig schema = schemaMap.get("solo1");
        CacheService cacheService = new CacheService();
        RouteService routerService = new RouteService(cacheService);

        // create table/view/function/..
        String sql = " create table company(idd int)";
        sql = RouterUtil.getFixedSql(sql);
        String upsql = sql.toUpperCase();

        //TODO：modify by zhuam 小写表名，转为大写比较
        String tablename =  RouterUtil.getTableName(sql, RouterUtil.getCreateTablePos(upsql, 0));
        tablename = tablename.toUpperCase();

        List<String> dataNodes = new ArrayList<>();
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        int nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }

        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        RouteResultset rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        // drop table test
        sql = " drop table if exists COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getDropTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();
        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //alter table
        sql = "   alter table COMPANY add COLUMN name int ;";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getAlterTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }
        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);

        //truncate table;
        sql = " truncate table COMPANY";
        sql = RouterUtil.getFixedSql(sql);
        upsql = sql.toUpperCase();
        tablename =  RouterUtil.getTableName(sql, RouterUtil.getTruncateTablePos(upsql, 0));
        tables = schema.getTables();
        if (tables != null && (tc = tables.get(tablename)) != null) {
            dataNodes = tc.getDataNodes();
        }
        nodeSize = dataNodes.size();

        if (nodeSize==0&& schema.getDataNode()!=null){
            nodeSize = 1;
        }

        rs = ServerParse.parse(sql);
        sqlType = rs & 0xff;
        rrs = routerService.route(new SystemConfig(), schema, sqlType, sql, "UTF-8", null);
        Assert.assertTrue("COMPANY".equals(tablename));
        Assert.assertTrue(rrs.getNodes().length == nodeSize);


    }



    @Test
    public void testTableMetaRead() throws Exception {
        final SchemaConfig schema = schemaMap.get("cndb");

        String sql = "desc offer";
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer", rrs.getNodes()[0].getStatement());

        sql = " desc cndb.offer col1";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.DESCRIBE, sql, null, null, cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("desc offer col1", rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  offer  IN db_name WHERE true";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());

        sql = "SHOW FULL COLUMNS FROM  db.offer  IN db_name WHERE true";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals(1, rrs.getNodes().length);
        // random return one node
        // Assert.assertEquals("offer_dn[0]", rrs.getNodes()[0].getName());
        Assert.assertEquals("SHOW FULL COLUMNS FROM offer WHERE true",
                rrs.getNodes()[0].getStatement());


        sql = "SHOW FULL TABLES FROM `TESTDB` WHERE Table_type != 'VIEW'";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
                cachePool);
        Assert.assertEquals(-1L, rrs.getLimitSize());
        Assert.assertEquals(false, rrs.isCacheAble());
        Assert.assertEquals("SHOW FULL TABLES WHERE Table_type != 'VIEW'", rrs.getNodes()[0].getStatement());

        sql = "SHOW INDEX  IN offer FROM  db_name";
        rrs = routeStrategy.route(new SystemConfig(), schema, ServerParse.SHOW, sql, null, null,
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
