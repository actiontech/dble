/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

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
}
