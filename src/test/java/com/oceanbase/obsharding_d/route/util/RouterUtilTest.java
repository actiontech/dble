/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/19
 */
public class RouterUtilTest {

    @Test
    public void testRemoveSchema() {
        String sql = "update test set name='abcdtestx.aa'   where id=1 and testx=123";
        String afterAql = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals(sql, afterAql);

    }

    @Test
    public void testRemoveSchemaSelect() {
        String sql = "select id as 'aa' from  test where name='abcdtestx.aa'   and id=1 and testx=123";
        String afterAql = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals(sql, afterAql);

    }

    @Test
    public void testRemoveSchemaSelect2() {
        String sql = "select id as 'aa' from  testx.test where name='abcd  testx.aa'   and id=1 and testx=123";
        String afterAql = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertNotSame(sql.indexOf("testx."), afterAql.indexOf("testx."));

    }

    @Test
    public void testRemoveSchema2() {
        String sql = "update testx.test set name='abcd \\' testx.aa' where id=1";
        String sqltrue = "update test set name='abcd \\' testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema3() {
        String sql = "update testx.test set testx.name='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema4() {
        String sql = "update testx.test set testx.name='abcd testx.aa' and testx.name2='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' and name2='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema5() {
        String sql = "UPDATE TESTX.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TESTX.NAME2='ABCD TESTX.AA' WHERE TESTX.ID=1";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema6() {
        String sql = "UPDATE TESTX.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TESTX.NAME2='ABCD TESTX.AA' WHERE TESTX.ID=1";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1";
        String sqlnew = RouterUtil.removeSchema(sql, "TESTX", false);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema7() {
        String sql = "update testx.test set testx.name='abcd testx.aa' and testx.name2='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' and name2='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", false);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema8() {
        String sql = "UPDATE TESTX.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TEStX.NAME2='ABCD TESTX.AA' WHERE testx.ID=1";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema9() {
        String sql = "UPDATE TESTX.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TEStX.NAME2='ABCD TESTX.AA' WHERE `testx`.ID=1";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema10() {
        String sql = "UPDATE TESTX.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TEStX.NAME2='ABCD TESTX.AA' WHERE `testx`.ID=1 OR testx.ID=2";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1 OR ID=2";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema11() {
        String sql = "UPDATE `TESTX`.TEST SET TESTX.NAME='ABCD TESTX.AA' AND TEStX.NAME2='ABCD TESTX.AA' WHERE testx.ID=1 OR `testx`.ID=2";
        String sqltrue = "UPDATE TEST SET NAME='ABCD TESTX.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1 OR ID=2";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema12() {
        String sql = "UPDATE `TESTX`.TEST SET TESTX.NAME='ABCD `TESTX`.AA' AND TEStX.NAME2='ABCD TESTX.AA' WHERE testx.ID=1 OR `testx`.ID=2";
        String sqltrue = "UPDATE TEST SET NAME='ABCD `TESTX`.AA' AND NAME2='ABCD TESTX.AA' WHERE ID=1 OR ID=2";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema13() {
        String sql = "UPDATE `TESTX`.TEST SET TESTX.NAME='ABCD \\' `TESTX`.AA' \\' AND TEStX.NAME2='ABCD TESTX.AA' WHERE testx.ID=1 OR `testx`.ID=2";
        String sqltrue = "UPDATE TEST SET NAME='ABCD \\' `TESTX`.AA' \\' AND NAME2='ABCD TESTX.AA' WHERE ID=1 OR ID=2";
        String sqlnew = RouterUtil.removeSchema(sql, "testx", true);
        Assert.assertEquals("EXECUTE ERROR:", sqltrue, sqlnew);
    }


    @Test
    public void testRemoveSchema14() {
        String sql = "select `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(1) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( `t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND `t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `t_order`.`brand` ASC,rpda_1 ASC";
        String afterAql = RouterUtil.removeSchema(sql, "order", true);
        Assert.assertEquals(sql, afterAql);
    }

    @Test
    public void testRemoveSchema15() {
        String sql1 = "select `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(order.t_order.brand) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( `t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND `t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `t_order`.`brand` ASC,rpda_1 ASC";
        String sql2 = "select `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(`order`.t_order.brand) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( `t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND `t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `t_order`.`brand` ASC,rpda_1 ASC";
        String sql3 = "select order.`t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(order.t_order.brand) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( `t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND order.`t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `t_order`.`brand` ASC,rpda_1 ASC";
        String sql4 = "select `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(`order`.t_order.brand) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(`order`.t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( `order`.`t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND `order`.`t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `order`.`t_order`.`brand` ASC,rpda_1 ASC";
        String sql5 = "select `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') as `time`,count(order.t_order.brand) as `_$COUNT$_rpda_0`," +
                "DATE_FORMAT(`order`.t_order.order_date, '%H:%i') as `rpda_1` from  `t_order` where  ( order.`t_order`.`brand` = 'hjh' " +
                "AND `t_order`.`order_date` > '2022-03-26 11:30:00' AND order.`t_order`.`order_date` < '2022-03-26:11:40:00') " +
                "GROUP BY `t_order`.`brand`,DATE_FORMAT(order_date, '%H:%i') ORDER BY `order`.`t_order`.`brand` ASC,rpda_1 ASC";
        String afterAql1 = RouterUtil.removeSchema(sql1, "order", true);
        String afterAql2 = RouterUtil.removeSchema(sql2, "order", true);
        String afterAql3 = RouterUtil.removeSchema(sql3, "order", true);
        String afterAql4 = RouterUtil.removeSchema(sql4, "order", true);
        String afterAql5 = RouterUtil.removeSchema(sql5, "order", true);
        Assert.assertNotEquals(sql1, afterAql1);
        Assert.assertEquals(afterAql1, afterAql2);
        Assert.assertEquals(afterAql2, afterAql3);
        Assert.assertEquals(afterAql3, afterAql4);
        Assert.assertEquals(afterAql4, afterAql5);
    }
}
