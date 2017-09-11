/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser.druid;

import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat.Condition;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
        List<List<Condition>> list = getConditionList(sql);
        Assert.assertEquals(list.size(), 5);
        Assert.assertEquals(list.get(0).size(), 2);
        Assert.assertEquals(list.get(1).size(), 2);
        Assert.assertEquals(list.get(2).size(), 3);
        Assert.assertEquals(list.get(3).size(), 4);
        Assert.assertEquals(list.get(4).size(), 3);

        Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.days = 5");
        Assert.assertEquals(list.get(0).get(1).toString(), "travelrecord.id = (1, 2)");

        Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.fee = 3");
        Assert.assertEquals(list.get(1).get(1).toString(), "travelrecord.id = (1, 2)");

        Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.fee = 0");
        Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
        Assert.assertEquals(list.get(2).get(2).toString(), "travelrecord.id = (1, 2)");

        Assert.assertEquals(list.get(3).get(0).toString(), "travelrecord.fee =");
        Assert.assertEquals(list.get(3).get(1).toString(), "travelrecord.days =");
        Assert.assertEquals(list.get(3).get(2).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
        Assert.assertEquals(list.get(3).get(3).toString(), "travelrecord.id = (1, 2)");

        Assert.assertEquals(list.get(4).get(0).toString(), "travelrecord.user_id = 2");
        Assert.assertEquals(list.get(4).get(1).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
        Assert.assertEquals(list.get(4).get(2).toString(), "travelrecord.id = (1, 2)");

        System.out.println(list.size());
    }

    /**
     * or
     */
    @Test
    public void test2() {
        String sql = "select id from travelrecord "
                + " where id = 1 and ( fee=3 or days=5 or name = 'zhangsan')";
        List<List<Condition>> list = getConditionList(sql);

        Assert.assertEquals(list.size(), 3);
        Assert.assertEquals(list.get(0).size(), 2);
        Assert.assertEquals(list.get(1).size(), 2);
        Assert.assertEquals(list.get(2).size(), 2);


        Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.name = zhangsan");
        Assert.assertEquals(list.get(0).get(1).toString(), "travelrecord.id = 1");

        Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.days = 5");
        Assert.assertEquals(list.get(1).get(1).toString(), "travelrecord.id = 1");

        Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.fee = 3");
        Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.id = 1");
    }

    /**
     * OR
     */
    @Test
    public void test3() {
        String sql = "select id from travelrecord "
                + " where id = 1 and fee=3 or days=5 or name = 'zhangsan'";
        List<List<Condition>> list = getConditionList(sql);

        Assert.assertEquals(list.size(), 3);

        Assert.assertEquals(list.get(0).size(), 1);
        Assert.assertEquals(list.get(1).size(), 1);
        Assert.assertEquals(list.get(2).size(), 2);

        Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.name = zhangsan");

        Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.days = 5");

        Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.id = 1");
        Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.fee = 3");
    }

    private List<List<Condition>> getConditionList(String sql) {
        SQLStatementParser parser = null;
        parser = new MySqlStatementParser(sql);

        ServerSchemaStatVisitor visitor = null;
        SQLStatement statement = null;
        //throw exception
        try {
            statement = parser.parseStatement();
            visitor = new ServerSchemaStatVisitor();
        } catch (Exception e) {
            e.printStackTrace();
        }
        statement.accept(visitor);

        List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
        if (visitor.hasOrCondition()) {//contains OR
            mergedConditionList = visitor.splitConditions();
        } else {
            mergedConditionList.add(visitor.getConditions());
        }

        return mergedConditionList;
    }
}
