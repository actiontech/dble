package com.actiontech.dble.route;

import com.actiontech.dble.route.impl.DefaultRouteStrategy;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DefaultDruidParser;
import com.alibaba.druid.sql.ast.SQLStatement;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2018/12/19.
 */
public class RouteWithOrTest {

    @Test
    public void test() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or name = 'ccc') and id = 12");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
    }

    @Test
    public void test1() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or (name = 'ccc' or name ='ddd')) and id =12");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(2);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
    }

    @Test
    public void test3() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where ((name = 'bbb' or name = 'ccc') or (name = 'eee' or (name = 'fff' or name ='ggg'))) and id = 12");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(2);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(3);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(4);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
    }


    @Test
    public void test4() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or name = 'ccc') and (id = 12 or id =14)");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(2);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(3);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
    }

    @Test
    public void test5() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or name = 'ccc') and id = 12 and pk=11");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
        unit = result.get(1);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
    }

    @Test
    public void test6() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or name = 'ccc') and (id = 12 OR ID = 13) and pk=11");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
        unit = result.get(1);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
        unit = result.get(2);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
        unit = result.get(3);
        Assert.assertEquals(3, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
    }


    @Test
    public void test7() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest where (name = 'bbb' or name = 'ccc') and (id = 12 OR id = 13) OR pk=11");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(1, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("PK"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(2);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(3);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(4);
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
    }


    @Test
    public void test8() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest,mtest where (ntest.name = 'cccc' or ntest.name = 'bbbb') and ntest.id = mtest.id and mtest.id = 1 ");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));

    }


    @Test
    public void test9() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest,mtest where ntest.id = mtest.id and mtest.id = 1 and (ntest.name = 'cccc' or ntest.name = 'bbbb') and (mtest.name = 'aaaa' or mtest.name = 'dddd')");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(1);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(2);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        unit = result.get(3);
        Assert.assertEquals(2, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));

    }


    @Test
    public void test10() {
        List<RouteCalculateUnit> result = getCoreCalculateUnit("select * from ntest,mtest,xtest where ntest.id = mtest.id and xtest.id = ntest.id and mtest.id = 1 and (ntest.name = 'cccc' or ntest.name = 'bbbb') and (mtest.name = 'aaaa' or mtest.name = 'dddd')");
        RouteCalculateUnit unit = result.get(0);
        Assert.assertEquals(3, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("xtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("xtest").containsKey("ID"));
        unit = result.get(1);
        Assert.assertEquals(3, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("xtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("xtest").containsKey("ID"));
        unit = result.get(2);
        Assert.assertEquals(3, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("xtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("xtest").containsKey("ID"));
        unit = result.get(3);
        Assert.assertEquals(3, unit.getTablesAndConditions().size());
        Assert.assertEquals(2, unit.getTablesAndConditions().get("ntest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(2, unit.getTablesAndConditions().get("mtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("ID"));
        Assert.assertTrue(unit.getTablesAndConditions().get("ntest").containsKey("NAME"));
        Assert.assertEquals(1, unit.getTablesAndConditions().get("xtest").size());
        Assert.assertTrue(unit.getTablesAndConditions().get("xtest").containsKey("ID"));

    }


    public List<RouteCalculateUnit> getCoreCalculateUnit(String sql) throws RuntimeException {
        try {
            ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
            DefaultRouteStrategy x = new DefaultRouteStrategy();
            SQLStatement statement = x.parserSQL(sql, null);
            statement.accept(visitor);

            DefaultDruidParser parser = new DefaultDruidParser();
            Class class1 = parser.getClass();
            Method format = class1.getDeclaredMethod("getTableAliasMap", Map.class);
            format.setAccessible(true);
            Map<String, String> tableAliasMap = (Map<String, String>) format.invoke(parser, visitor.getAliasMap());

            Method format2 = class1.getDeclaredMethod("buildRouteCalculateUnits", Map.class, List.class);
            format2.setAccessible(true);
            List<RouteCalculateUnit> result = (List<RouteCalculateUnit>) format2.invoke(parser, tableAliasMap, visitor.getConditionList());
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
