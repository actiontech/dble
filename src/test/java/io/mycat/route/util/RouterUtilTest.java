package io.mycat.route.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/19
 */
public class RouterUtilTest {




    @Test
    public void testRemoveSchema()  {
        String sql = "update test set name='abcdtestx.aa'   where id=1 and testx=123";

      String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertEquals(sql,afterAql);
        System.out.println(afterAql);

    }
    @Test
    public void testRemoveSchemaSelect()  {
        String sql = "select id as 'aa' from  test where name='abcdtestx.aa'   and id=1 and testx=123";

        String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertEquals(sql,afterAql);

    }

    @Test
    public void testRemoveSchemaSelect2()  {
        String sql = "select id as 'aa' from  testx.test where name='abcd  testx.aa'   and id=1 and testx=123";

        String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertNotSame(sql.indexOf("testx."),afterAql.indexOf("testx."));

    }

    @Test
    public void testRemoveSchema2(){
        String sql = "update testx.test set name='abcd \\' testx.aa' where id=1";
        String sqltrue = "update test set name='abcd \\' testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema3(){
        String sql = "update testx.test set testx.name='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema4(){
        String sql = "update testx.test set testx.name='abcd testx.aa' and testx.name2='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' and name2='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }
}
