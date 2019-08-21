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
        Assert.assertEquals(ServerParse.FLUSH, sqlType);
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
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = '\\'sdfsd;f\\'';";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd;f\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd';'f\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd\\\";'f\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = 'sdfsd\";\"f';";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = 'sdfsd\";f';";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        String sql1 = "delete from tablex where name = '''sdfsd;f''';";
        String sql2 = "commit;";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));

        sql1 = "commit;";
        sql2 = "delete from tablex where name = '''sdfsd;f''';";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));

        sql1 = "sdfsdf\\' ;";
        sql2 = "delete from tablex where name = '''sdfsd;f''';";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));

        sql = "delete from tablex where name = 'sdfsd;f';";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));
        sql = "delete from tablex where name = 'sdfsdf';";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "delete from tablex where name = \"sdfsd\\\\\\\";'f\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "update char_columns set c_char ='1',c_char2=\"2\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "update char_columns set c_char ='1;',c_char2=\";2\";";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "/*!dble:sql=select 1 from account */create procedure proc_test(userid1 int)\n" +
                "begin\n" +
                "  insert into xx select * from xxx where userid=userid1;\n" +
                "  update xx set yy=true,zz_time=now() where userid=userid1;\n" +
                "end;";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql = "/*!dble:sql=select 1 from account */create procedure proc_test(userid1 int)\n" +
                "select * from char_columns;";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));

        sql1 = "/*!dble:sql=select 1 from account */create procedure proc_test(userid1 int)\n" +
                "select * from char_columns;";
        sql2 = "select * from char_columns;";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));

        sql1 = "/*!dble:sql=select 1 from account */create procedure proc_arc(userid1 int)\n" +
                "begin\n" +
                "  insert into xx select * from xxx where userid=userid1;\n" +
                "  update xx set yy=true,zz_time=now() where userid=userid1;\n" +
                "end;";
        sql2 = "select * from char_columns ;";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));


        sql1 = "/*!dble:sql=select 1 from account */create procedure proc_arc(userid1 int)\n" +
                "begin\n" +
                "  select 'GET the PART 1 END;'\n" +
                "  update xx set yy=true,zz_time=now() where userid=userid1;\n" +
                "end;";
        sql2 = "select * from char_columns ;";
        Assert.assertEquals(sql1.length() - 1, ParseUtil.findNextBreak(sql1 + sql2));


        sql = "INSERT INTO TEST_TABLE SET VALUE = \"/*!dble:sql=select 1 from account */create procedure proc_arc(userid1 int)\n" +
                "begin\n" +
                "  select 'GET the PART 1 END;'\n" +
                "  update xx set yy=true,zz_time=now() where userid=userid1;\n" +
                "end;\" ,ID = 11111";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));


        sql = "CREATE FUNCTION CustomerLevel(p_creditLimit double) RETURNS VARCHAR(10)\n" +
                "    DETERMINISTIC\n" +
                "BEGIN\n" +
                "    DECLARE lvl varchar(10);\n" +
                "    IF p_creditLimit > 50000 THEN\n" +
                " SET lvl = 'PLATINUM';\n" +
                "    ELSEIF (p_creditLimit <= 50000 AND p_creditLimit >= 10000) THEN\n" +
                "        SET lvl = 'GOLD';\n" +
                "    ELSEIF p_creditLimit < 10000 THEN\n" +
                "        SET lvl = 'SILVER';\n" +
                "    END IF;\n" +
                " RETURN (lvl);\n" +
                "END";
        Assert.assertEquals(sql.length() - 1, ParseUtil.findNextBreak(sql));
    }
}
