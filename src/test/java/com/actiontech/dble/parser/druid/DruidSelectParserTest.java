/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser.druid;

import com.actiontech.dble.route.parser.druid.impl.DruidSelectParser;
import com.actiontech.dble.route.parser.util.DruidUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Hash Zhang on 2016/4/29.
 * Modified by Hash Zhang on 2016/5/25 add testGroupByWithViewAlias.
 */
public class DruidSelectParserTest {
    DruidSelectParser druidSelectParser = new DruidSelectParser();

    /**
     * testGroupByWithAlias
     * if rbuildGroupByCols of DruidSelectParse change the column
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGroupByWithAlias() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String functionColumn = "DATE_FORMAT(h.times,'%b %d %Y %h:%i %p')";
        Object result = invokeGroupBy(functionColumn);
        Assert.assertEquals(functionColumn, ((String[]) result)[0]);
    }

    /**
     * testGroupByWithViewAlias
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGroupByWithViewAlias() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String functionColumn = "select id from (select h.id from hotnews h  union select h.title from hotnews h ) as t1 group by t1.id;";
        Object result = invokeGroupBy(functionColumn);
        Assert.assertEquals(functionColumn, ((String[]) result)[0]);
    }

    public Object invokeGroupBy(String functionColumn) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Map<String, String> aliaColumns = new TreeMap<>();
        SQLIdentifierExpr sqlExpr = new SQLIdentifierExpr(functionColumn);
        List<SQLExpr> groupByItems = new ArrayList<>();
        groupByItems.add(sqlExpr);
        Class c = DruidSelectParser.class;
        Method method = c.getDeclaredMethod("buildGroupByCols", new Class[]{List.class, Map.class});
        method.setAccessible(true);
        return method.invoke(druidSelectParser, groupByItems, aliaColumns);
    }

    @Test
    public void testAPNotSupport() throws SQLException {
        String origSQL1 = "select ALL id from sbtest1";
        String origSQL2 = "select DISTINCTROW id from sbtest1";
        String origSQL3 = "select HIGH_PRIORITY id from sbtest1";
        String origSQL4 = "select STRAIGHT_JOIN id from sbtest1";
        String origSQL5 = "select SQL_SMALL_RESULT id from sbtest1";
        String origSQL6 = "select SQL_BIG_RESULT id from sbtest1";
        String origSQL7 = "select SQL_BUFFER_RESULT id from sbtest1";
        String origSQL8 = "select SQL_NO_CACHE id from sbtest1";
        String origSQL9 = "select SQL_CALC_FOUND_ROWS id from sbtest1";
        String origSQL10 = "select id,RANK() OVER w AS 'rank' from sbtest1 WINDOW w AS (ORDER BY id)";
        String origSQL11 = "select id from sbtest1 for update";
        String origSQL12 = "select id from sbtest1 for share";
        String origSQL13 = "select id from sbtest1 for update NOWAIT";
        String origSQL14 = "select id from sbtest1 for update SKIP LOCKED";
        String origSQL15 = "select id from sbtest1 LOCK IN SHARE MODE";
        String origSQL16 = "select id,@a from sbtest1";
        String origSQL17 = "select id from sbtest1 where id = @a";
        ArrayList<String> list = Lists.newArrayList(origSQL1, origSQL2, origSQL3, origSQL4, origSQL5, origSQL6, origSQL7, origSQL8, origSQL9, origSQL10, origSQL11, origSQL12, origSQL13, origSQL14, origSQL15, origSQL16, origSQL17);
        for (String sql : list) {
            SQLStatement stmt = DruidUtil.parseSQL(sql);
            MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
            boolean notSupport = RouterUtil.checkSQLNotSupport(query);
            Assert.assertTrue(notSupport);
        }
    }

    @Test
    public void testAPNotSupport_join() throws SQLException {
        String sql = "select id from sbtest1 a union all select ALL id from sbtest1";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean notSupport = RouterUtil.checkSQLNotSupport(query);
        Assert.assertTrue(notSupport);
    }


    @Test
    public void testAPNotSupport_subQuery_union() throws SQLException {
        String sql = "select a.* from (select * from sbtest1 LOCK IN SHARE MODE) a union select * from sbtest1";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean notSupport = RouterUtil.checkSQLNotSupport(query);
        Assert.assertTrue(notSupport);
    }

    @Test
    public void testAPNotSupport_variables() throws SQLException {
        String sql = "select a.* from (select * from sbtest1 where id = @a) a union select @a from sbtest1";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean notSupport = RouterUtil.checkSQLNotSupport(query);
        Assert.assertTrue(notSupport);
    }

    @Test
    public void testOLAP() throws SQLException {
        String origSQL1 = "select VAR_SAMP(id) from sbtest1";
        String origSQL2 = "select min(id) from sbtest1";
        String origSQL3 = "select sum(id) from sbtest1";
        String origSQL4 = "select max(id) from sbtest1";
        String origSQL5 = "select count(id) from sbtest1";
        String origSQL6 = "select avg(id) from sbtest1";
        String origSQL7 = "select STDDEV_POP(id) from sbtest1";
        String origSQL8 = "select STDDEV_SAMP(id) from sbtest1";
        String origSQL9 = "select VAR_POP(id) from sbtest1";
        String origSQL10 = "select VAR_SAMP(id) from sbtest1";
        String origSQL11 = "select id from sbtest1 group by id";
        String origSQL12 = "select id from (select min(id) as id from sbtest1) a";
        ArrayList<String> list = Lists.newArrayList(origSQL1, origSQL2, origSQL3, origSQL4, origSQL5, origSQL6, origSQL7, origSQL8, origSQL9, origSQL10, origSQL11, origSQL12);
        for (String sql : list) {
            SQLStatement stmt = DruidUtil.parseSQL(sql);
            MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
            boolean checkFunction = RouterUtil.checkFunction(query);
            Assert.assertTrue(checkFunction);
        }
    }

    @Test
    public void testOLAP_subQuery() throws SQLException {
        String sql = "select b.id from (select a.id from (select min(id) as id from sbtest1) a) b";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_join() throws SQLException {
        String sql = "select a.id,min(b.id) from table_1 a join sbtest1 b on a.id = b.id";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_union() throws SQLException {
        String sql = "select a.id from table_1 a union all select b.id from table_1 b union all select min(c.id) from sbtest1 c";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_join_subQuery() throws SQLException {
        String sql = "select a.id,b.id from table_1 a join (select min(id) as id from sbtest1) b on a.id = b.id";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_subQuery_join() throws SQLException {
        String sql = "select c.id from (select a.id,min(b.id) from table_1 a join sbtest1 b on a.id = b.id) c";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_subQuery_union() throws SQLException {
        String sql = "select c.id from (select a.id from table_1 a union all select min(b.id) from sbtest1 b) c";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }

    @Test
    public void testOLAP_union_subQuery() throws SQLException {
        String sql = "select a.id from table_1 a union all select b.id from (select VAR_SAMP(id) as id from sbtest1) b";
        SQLStatement stmt = DruidUtil.parseSQL(sql);
        SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
        boolean checkFunction = RouterUtil.checkFunction(query);
        Assert.assertTrue(checkFunction);
    }
}