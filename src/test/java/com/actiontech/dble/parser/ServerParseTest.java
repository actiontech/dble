/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.parser.ServerParse;
import org.junit.Assert;
import org.junit.Test;

public class ServerParseTest {
    /**
     * public static final int OTHER = -1;
     * public static final int BEGIN = 1;
     * public static final int COMMIT = 2;
     * public static final int DELETE = 3;
     * public static final int INSERT = 4;
     * public static final int REPLACE = 5;
     * public static final int ROLLBACK = 6;
     * public static final int SELECT = 7;
     * public static final int SET = 8;
     * public static final int SHOW = 9;
     * public static final int START = 10;
     * public static final int UPDATE = 11;
     * public static final int KILL = 12;
     * public static final int SAVEPOINT = 13;
     * public static final int USE = 14;
     * public static final int EXPLAIN = 15;
     * public static final int KILL_QUERY = 16;
     * public static final int HELP = 17;
     * public static final int MYSQL_CMD_COMMENT = 18;
     * public static final int MYSQL_COMMENT = 19;
     * public static final int CALL = 20;
     * public static final int DESCRIBE = 21;
     * <p>
     * public static final int SCRIPT_PREPARE = 101;
     */

    @Test
    public void testDesc() {
        String sql = "desc a";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.DESCRIBE, sqlType);
    }

    @Test
    public void testDescribe() {
        String sql = "describe a";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.DESCRIBE, sqlType);
    }

    @Test
    public void testDelete() {
        String sql = "delete from a where id = 1";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.DELETE, sqlType);
    }

    @Test
    public void testDroprepare() {
        String sql = "drop prepare stmt";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, sqlType);
    }

    @Test
    public void testAnalyze() {
        String sql = "analyze table t1;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testChecksum() {
        String sql = "checksum table table_name;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testFlush() {
        String sql = "flush table table_name;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testLoadindex() {
        String sql = "LOAD INDEX INTO CACHE t1;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testOptimize() {
        String sql = "optimize table table_name;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testRename() {
        String sql = "rename table table_name1 to table_name2;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testRepair() {
        String sql = "repair table table_name;";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UNSUPPORT, sqlType);
    }

    @Test
    public void testDeallocate() {
        String sql = "Deallocate prepare stmt";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, sqlType);
    }

    @Test
    public void testInsert() {
        String sql = "insert into a(name) values ('zhangsan')";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.INSERT, sqlType);
    }

    @Test
    public void testReplace() {
        String sql = "replace into t(id, update_time) select 1, now();  ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.REPLACE, sqlType);
    }

    @Test
    public void testPrepare() {
        String sql = "prepare stmt from 'select * from aa where id=?'";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, sqlType);
    }

    @Test
    public void testSet() {
        String sql = "SET @var_name = 'value';  ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SET, sqlType);
    }

    @Test
    public void testShow() {
        String sql = "show full tables";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SHOW, sqlType);
    }

    @Test
    public void testStart() {
        String sql = "start ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.START, sqlType);
    }

    @Test
    public void testUpdate() {
        String sql = "update a set name='wdw' where id = 1";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.UPDATE, sqlType);
    }

    @Test
    public void testKill() {
        String sql = "kill 1";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.KILL, sqlType);
    }

    @Test
    public void testSavePoint() {
        String sql = "SAVEPOINT ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SAVEPOINT, sqlType);
    }

    @Test
    public void testUse() {
        String sql = "use db1 ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.USE, sqlType);
    }

    @Test
    public void testExplain() {
        String sql = "explain select * from a ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.EXPLAIN, sqlType);
    }

    @Test
    public void testExecute() {
        String sql = "execute stmt using @f1, f2";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, sqlType);
    }

    @Test
    public void testKillQuery() {
        String sql = "kill query 1102 ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.KILL_QUERY, sqlType);
    }

    @Test
    public void testHelp() {
        String sql = "help contents ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.HELP, sqlType);
    }

    @Test
    public void testMysqlCmdComment() {

    }

    @Test
    public void testMysqlComment() {

    }

    @Test
    public void testCall() {
        String sql = "CALL demo_in_parameter(@p_in); ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.CALL, sqlType);
    }

    @Test
    public void testRollback() {
        String sql = "rollback; ";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.ROLLBACK, sqlType);
    }

    @Test
    public void testSelect() {
        String sql = "select * from a";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.SELECT, sqlType);
    }

    @Test
    public void testBegin() {
        String sql = "begin";
        int result = ServerParse.parse(sql);
        int sqlType = result & 0xff;
        Assert.assertEquals(ServerParse.BEGIN, sqlType);
    }

    @Test
    public void testCommit() {
        String sql = "COMMIT 'nihao'";
        int result = ServerParse.parse(sql);
        Assert.assertEquals(ServerParse.OTHER, result);
    }


    @Test
    public void testDivide() {
        String sql = "delete from tablex where name = '''sdfsd;f''';";
        Assert.assertEquals(45, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = '\\'sdfsd;f\\'';";
        Assert.assertEquals(45, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd;f\";";
        Assert.assertEquals(41, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd';'f\";";
        Assert.assertEquals(43, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd\\\";'f\";";
        Assert.assertEquals(44, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = 'sdfsd\";\"f';";
        Assert.assertEquals(43, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = 'sdfsd\";f';";
        Assert.assertEquals(42, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = '''sdfsd;f''';commit;";
        Assert.assertEquals(45, ParseUtil.findNextBreak(sql));

        sql = "commit;delete from tablex where name = '''sdfsd;f''';";
        Assert.assertEquals(6, ParseUtil.findNextBreak(sql));

        sql = "sdfsdf\\' ;delete from tablex where name = '''sdfsd;f''';";
        Assert.assertEquals(9, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = 'sdfsd;f';";
        Assert.assertEquals(41, ParseUtil.findNextBreak(sql));
        sql = "delete from tablex where name = 'sdfsdf';";
        Assert.assertEquals(40, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd\\\\\\\";'f\";";
        Assert.assertEquals(46, ParseUtil.findNextBreak(sql));

        sql = "update char_columns set c_char ='1',c_char2=\"2\";";
        Assert.assertEquals(47, ParseUtil.findNextBreak(sql));

        sql = "update char_columns set c_char ='1;',c_char2=\";2\";";
        Assert.assertEquals(49, ParseUtil.findNextBreak(sql));


    }
}
