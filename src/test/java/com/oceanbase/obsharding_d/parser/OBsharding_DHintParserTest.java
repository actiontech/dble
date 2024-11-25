/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.parser;

import com.oceanbase.obsharding_d.route.parser.OBsharding_DHintParser;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;

/**
 * @author mycat
 */
public class OBsharding_DHintParserTest {

    @Test
    public void testOBsharding_DHint() throws SQLSyntaxErrorException {

        OBsharding_DHintParser.HintInfo hintInfo;
        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:sql  =   select * from sbtest */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.SQL, hintInfo.getType());
        Assert.assertEquals("select * from sbtest", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:shardingNode =dn5  */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.SHARDING_NODE, hintInfo.getType());
        Assert.assertEquals("dn5", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:shardingNode =' dn5 '  */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.SHARDING_NODE, hintInfo.getType());
        Assert.assertEquals(" dn5 ", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:db_type= master */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.DB_TYPE, hintInfo.getType());
        Assert.assertEquals("master", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:db_instance_url = 127.0.0.1:3307 */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.DB_INSTANCE_URL, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/*!OBsharding-D:db_instance_url=127.0.0.1:3307 */");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.DB_INSTANCE_URL, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("", hintInfo.getRealSql());


        hintInfo = OBsharding_DHintParser.parse("/*#OBsharding-D:plan=a&b&c$inner2left$in2join */ sss");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.PLAN, hintInfo.getType());
        Assert.assertEquals("a&b&c$inner2left$in2join", hintInfo.getHintValue());
        Assert.assertEquals("sss", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parse("/ * 127.0.0.1:3307 */ sss");
        Assert.assertNull(hintInfo);

        try {
            OBsharding_DHintParser.parse("/*!OBsharding-D:db_instance_url=127.0.0.1:3307 *");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("please following the OBsharding-D hint syntax"));
        }

        try {
            OBsharding_DHintParser.parse("/*!OBsharding-D:db_instance_url     */");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("please following the OBsharding-D hint syntax"));
        }

        try {
            OBsharding_DHintParser.parse("/*#OBsharding-D:plan a&b */");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("please following the OBsharding-D hint syntax"));
        }


    }

    @Test
    public void testUproxyHint() throws SQLSyntaxErrorException {
        OBsharding_DHintParser.HintInfo hintInfo;
        hintInfo = OBsharding_DHintParser.parseRW("insert into test values/* master */ (11111)");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.UPROXY_MASTER, hintInfo.getType());
        Assert.assertEquals("master", hintInfo.getHintValue());
        Assert.assertEquals("insert into test values/* master */ (11111)", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parseRW("insert into /* uproxy_dest: 127.0.0.1:3307 */  test values (11111)");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(OBsharding_DHintParser.UPROXY_DEST, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("insert into /* uproxy_dest: 127.0.0.1:3307 */  test values (11111)", hintInfo.getRealSql());

        hintInfo = OBsharding_DHintParser.parseRW("insert into /* uproxy_dest : 127.0.0.1:3307 */  test values (11111)");
        Assert.assertNull(hintInfo);
    }

}
