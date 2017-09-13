/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route;

import com.actiontech.dble.SimpleCachePool;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


@Ignore
public class DruidMysqlCreateTableTest {
    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;
    protected ServerConnection sc = new ServerConnection();
    private static final String originSql1 = "CREATE TABLE autoslot"
            + "("
            + "	ID BIGINT AUTO_INCREMENT,"
            + "	CHANNEL_ID INT(11),"
            + "	CHANNEL_INFO varchar(128),"
            + "	CONSTRAINT RETL_MARK_ID PRIMARY KEY (ID)"
            + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";


    public DruidMysqlCreateTableTest() {
        //		String schemaFile = "/route/schema.xml";
        //		String ruleFile = "/route/rule.xml";
        //		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile); \
        //		schemaMap = schemaLoader.getSchemas();
        ServerConfig cnf = new ServerConfig();
        schemaMap = cnf.getSchemas();

        routeStrategy = RouteStrategyFactory.getRouteStrategy();

        sc.setUser("test");
    }

    @Test
    public void testCreate() throws SQLException {

        SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(schema, -1, originSql1, sc, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
        String sql = rrs.getNodes()[0].getStatement();

        //Assert.assertTrue(parseSql(sql));
        Assert.assertFalse(parseSql(sql));
    }

    // @Test
    public void testInsert() throws SQLException {
        SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(schema, -1, "insert into autoslot (id,sid) values(1,2) ", sc, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);

        //Assert.assertTrue(isInsertHasSlot(rrs.getStatement()));
        Assert.assertFalse(isInsertHasSlot(rrs.getStatement()));
    }

    private boolean isInsertHasSlot(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        MySqlInsertStatement insertStatement = (MySqlInsertStatement) parser.parseStatement();
        List<SQLExpr> cc = insertStatement.getColumns();
        for (SQLExpr sqlExpr : cc) {
            SQLIdentifierExpr c = (SQLIdentifierExpr) sqlExpr;
            if ("_slot".equalsIgnoreCase(c.getName()) && cc.size() == insertStatement.getValues().getValues().size())
                return true;
        }
        return false;
    }

    public boolean parseSql(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        return hasColumn(statement);
    }

    private static boolean hasColumn(SQLStatement statement) {
        for (SQLTableElement tableElement : ((SQLCreateTableStatement) statement).getTableElementList()) {
            SQLName sqlName = null;
            if (tableElement instanceof SQLColumnDefinition) {
                sqlName = ((SQLColumnDefinition) tableElement).getName();
            }

            if (sqlName != null) {
                String simpleName = sqlName.getSimpleName();
                simpleName = StringUtil.removeBackQuote(simpleName);
                if (tableElement instanceof SQLColumnDefinition && "_slot".equalsIgnoreCase(simpleName)) {
                    return true;
                }
            }
        }
        return false;
    }
}
