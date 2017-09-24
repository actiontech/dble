/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser.druid;

import com.actiontech.dble.route.parser.druid.impl.DruidSelectParser;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        SQLIdentifierExpr sqlExpr = mock(SQLIdentifierExpr.class);
        SQLIdentifierExpr expr = mock(SQLIdentifierExpr.class);
        List<SQLExpr> groupByItems = new ArrayList<>();
        groupByItems.add(sqlExpr);
        when((sqlExpr).getName()).thenReturn(functionColumn);
        Class c = DruidSelectParser.class;
        Method method = c.getDeclaredMethod("buildGroupByCols", new Class[]{List.class, Map.class});
        method.setAccessible(true);
        return method.invoke(druidSelectParser, groupByItems, aliaColumns);
    }


}