/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser.druid;

import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.WhereUnit;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat.Condition;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author user
 * @version 0.1.0
 * @date 2015-6-2 17:50:25
 * @copyright wonhigh.cn
 */
public class ServerSchemaStatVisitorTest {

    /**
     * 3 nest or
     */
    @Test
    public void test1() {
        String sql = "select id from travelrecord "
                + " where id = 1 and ( fee=3 or days=5 or (traveldate = '2015-05-04 00:00:07.375' "
                + " and (user_id=2 or fee=days or fee = 0))) and id=2";
        List<WhereUnit> whereUnits = getAllWhereUnit(sql);
        WhereUnit whereUnit = whereUnits.get(0);
        List<Condition> list1 = whereUnit.getOutAndConditions();

        Assert.assertEquals(list1.get(0).toString(), "travelrecord.id = 1");
        Assert.assertEquals(list1.get(1).toString(), "travelrecord.id = 2");

        WhereUnit childWhereUnits = whereUnit.getSubWhereUnit().get(0);
        List<List<Condition>> childList = childWhereUnits.getOrConditionList();

        Assert.assertEquals(childList.get(0).get(0).toString(), "travelrecord.days = 5");
        Assert.assertEquals(childList.get(1).get(0).toString(), "travelrecord.fee = 3");

        WhereUnit child2WhereUnits = childWhereUnits.getSubWhereUnit().get(0);
        List<Condition> child2 = child2WhereUnits.getOutAndConditions();
        Assert.assertEquals(child2.get(0).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");

        WhereUnit child3WhereUnits = child2WhereUnits.getSubWhereUnit().get(0);
        List<List<Condition>> child3List = child3WhereUnits.getOrConditionList();
        Assert.assertEquals(child3List.get(0).get(0).toString(), "travelrecord.fee = 0");
        Assert.assertEquals(child3List.get(1).size(), 0);
        Assert.assertEquals(child3List.get(2).get(0).toString(), "travelrecord.user_id = 2");

    }

    /**
     * or
     */
    @Test
    public void test2() {
        String sql = "select id from travelrecord "
                + " where id = 1 and ( fee=3 or days=5 or name = 'zhangsan')";
        List<WhereUnit> whereUnits = getAllWhereUnit(sql);
        WhereUnit whereUnit = whereUnits.get(0);
        List<Condition> list1 = whereUnit.getOutAndConditions();
        Assert.assertEquals(list1.get(0).toString(), "travelrecord.id = 1");

        WhereUnit childWhereUnits = whereUnit.getSubWhereUnit().get(0);
        List<List<Condition>> childList = childWhereUnits.getOrConditionList();

        Assert.assertEquals(childList.get(0).get(0).toString(), "travelrecord.name = zhangsan");
        Assert.assertEquals(childList.get(1).get(0).toString(), "travelrecord.days = 5");
        Assert.assertEquals(childList.get(2).get(0).toString(), "travelrecord.fee = 3");

    }

    /**
     * OR
     */
    @Test
    public void test3() {
        String sql = "select id from travelrecord "
                + " where id = 1 and fee=3 or days=5 or name = 'zhangsan'";
        List<WhereUnit> whereUnits = getAllWhereUnit(sql);
        List<List<Condition>> list = whereUnits.get(0).getOrConditionList();
        Assert.assertEquals(list.size(), 3);

        Assert.assertEquals(list.get(0).size(), 1);
        Assert.assertEquals(list.get(1).size(), 1);
        Assert.assertEquals(list.get(2).size(), 2);

        Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.name = zhangsan");

        Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.days = 5");

        Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.id = 1");
        Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.fee = 3");
    }

    private List<WhereUnit> getAllWhereUnit(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);

        ServerSchemaStatVisitor visitor;
        //throw exception
        try {
            SQLStatement statement = parser.parseStatement();
            visitor = new ServerSchemaStatVisitor();
            statement.accept(visitor);
            return visitor.getAllWhereUnit();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  null;
    }
}
