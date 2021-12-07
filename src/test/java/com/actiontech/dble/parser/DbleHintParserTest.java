/*
 * Copyright (C) 2016-2019 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.parser;

import com.actiontech.dble.route.parser.DbleHintParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat
 */
public class DbleHintParserTest {

    @Test
    public void testDbleHint() {

        DbleHintParser.HintInfo hintInfo;
        hintInfo = DbleHintParser.parse("/*!dble:sql  =   select * from sbtest */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.SQL, hintInfo.getType());
        Assert.assertEquals("select * from sbtest", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:shardingNode =dn5  */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.SHARDING_NODE, hintInfo.getType());
        Assert.assertEquals("dn5", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:shardingNode =' dn5 '  */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.SHARDING_NODE, hintInfo.getType());
        Assert.assertEquals(" dn5 ", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:db_type= master */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.DB_TYPE, hintInfo.getType());
        Assert.assertEquals("master", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:db_instance_url = 127.0.0.1:3307 */ call p_show_time()");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.DB_INSTANCE_URL, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("call p_show_time()", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:db_instance_url=127.0.0.1:3307 */");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.DB_INSTANCE_URL, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("", hintInfo.getRealSql());


        hintInfo = DbleHintParser.parse("/*#dble:plan=a&b&c$inner2left$in2join */ sss");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.PLAN, hintInfo.getType());
        Assert.assertEquals("a&b&c$inner2left$in2join", hintInfo.getHintValue());
        Assert.assertEquals("sss", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parse("/*!dble:db_instance_url=127.0.0.1:3307 *");
        Assert.assertNull(hintInfo);
    }

    @Test
    public void testUproxyHint() {
        DbleHintParser.HintInfo hintInfo;
        hintInfo = DbleHintParser.parseRW("insert into test values/* master */ (11111)");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.UPROXY_MASTER, hintInfo.getType());
        Assert.assertEquals("master", hintInfo.getHintValue());
        Assert.assertEquals("insert into test values/* master */ (11111)", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parseRW("insert into /* uproxy_dest: 127.0.0.1:3307 */  test values (11111)");
        Assert.assertNotNull(hintInfo);
        Assert.assertEquals(DbleHintParser.UPROXY_DEST, hintInfo.getType());
        Assert.assertEquals("127.0.0.1:3307", hintInfo.getHintValue());
        Assert.assertEquals("insert into /* uproxy_dest: 127.0.0.1:3307 */  test values (11111)", hintInfo.getRealSql());

        hintInfo = DbleHintParser.parseRW("insert into /* uproxy_dest : 127.0.0.1:3307 */  test values (11111)");
        Assert.assertNull(hintInfo);
    }

}
