/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.parser;

import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.server.parser.ServerParseSelect;
import com.oceanbase.obsharding_d.server.parser.ServerParseShow;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat
 */
public class ServerParserTest {

    ServerParse serverParse = ServerParseFactory.getShardingParser();

    @Test
    public void testIsBegin() {
        Assert.assertEquals(ServerParse.BEGIN, serverParse.parse("begin"));
        Assert.assertEquals(ServerParse.BEGIN, serverParse.parse("BEGIN"));
        Assert.assertEquals(ServerParse.BEGIN, serverParse.parse("BegIn"));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("BegIn X"));
    }

    @Test
    public void testIsCommit() {
        Assert.assertEquals(ServerParse.COMMIT, serverParse.parse("commit"));
        Assert.assertEquals(ServerParse.COMMIT, serverParse.parse("COMMIT"));
        Assert.assertEquals(ServerParse.COMMIT, serverParse.parse("cOmmiT "));
    }


    @Test
    public void testComment() {
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, serverParse.parse("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, serverParse.parse("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, serverParse.parse("/*!40101 SET @saved_cs_client     = @@character_set_client */"));

        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("/*SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("/*SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("/*SET @saved_cs_client     = @@character_set_client */"));
    }

    @Test
    public void testHintComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & serverParse.parse("/*#OBsharding-D:sharding=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & serverParse.parse("/*#OBsharding-D: sharding = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & serverParse.parse("/*#OBsharding-D: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testOldHintComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & serverParse.parse("/*!OBsharding-D:sharding=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & serverParse.parse("/*!OBsharding-D: sharding = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & serverParse.parse("/*!OBsharding-D: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testDoubleDashComment() {
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("--     "));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("--\t    "));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("- \n"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, serverParse.parse("--         select * from test"));
        Assert.assertEquals(ServerParse.INSERT, serverParse.parse("-- select *\n-- fdfadsfad\ninsert into test values(1)"));
        Assert.assertEquals(ServerParse.INSERT, serverParse.parse("--\tselect *\n-- fdfadsfad\ninsert into test values(1)"));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("-- select *\nfdfadsfad\ninsert into test values(1)"));
    }

    @Test
    public void testIsDelete() {
        Assert.assertEquals(ServerParse.DELETE, serverParse.parse("delete ..."));
        Assert.assertEquals(ServerParse.DELETE, serverParse.parse("DELETE ..."));
        Assert.assertEquals(ServerParse.DELETE, serverParse.parse("DeletE ..."));
    }

    @Test
    public void testIsInsert() {
        Assert.assertEquals(ServerParse.INSERT, serverParse.parse("insert ..."));
        Assert.assertEquals(ServerParse.INSERT, serverParse.parse("INSERT ..."));
        Assert.assertEquals(ServerParse.INSERT, serverParse.parse("InserT ..."));
    }

    @Test
    public void testIsReplace() {
        Assert.assertEquals(ServerParse.REPLACE, serverParse.parse("replace ..."));
        Assert.assertEquals(ServerParse.REPLACE, serverParse.parse("REPLACE ..."));
        Assert.assertEquals(ServerParse.REPLACE, serverParse.parse("rEPLACe ..."));
    }

    @Test
    public void testIsRollback() {
        Assert.assertEquals(ServerParse.ROLLBACK, serverParse.parse("rollback"));
        Assert.assertEquals(ServerParse.ROLLBACK, serverParse.parse("rollback  work   /* OBsharding-D_dest_expect:M */"));
        Assert.assertEquals(ServerParse.ROLLBACK, serverParse.parse("ROLLBACK"));
        Assert.assertEquals(ServerParse.ROLLBACK, serverParse.parse("rolLBACK "));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("rolLBACK @@xxxx"));
    }

    @Test
    public void testIsSelect() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & serverParse.parse("select ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & serverParse.parse("SELECT ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & serverParse.parse("sELECt ..."));
    }

    @Test
    public void testIsSet() {
        Assert.assertEquals(ServerParse.SET, 0xff & serverParse.parse("set ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & serverParse.parse("SET ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & serverParse.parse("sEt ..."));
    }

    @Test
    public void testIsShow() {
        Assert.assertEquals(ServerParse.SHOW, 0xff & serverParse.parse("show ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & serverParse.parse("SHOW ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & serverParse.parse("sHOw ..."));
    }

    @Test
    public void testIsStart() {
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("start ..."));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("START ..."));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse("stART ..."));
    }

    @Test
    public void testIsUpdate() {
        Assert.assertEquals(ServerParse.UPDATE, serverParse.parse("update ..."));
        Assert.assertEquals(ServerParse.UPDATE, serverParse.parse("UPDATE ..."));
        Assert.assertEquals(ServerParse.UPDATE, serverParse.parse("UPDate ..."));
    }

    @Test
    public void testIsShowDatabases() {
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("SHOW DATABASES", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("SHOW databases ", 4));
        //"show database " statement need stricter inspection #1961
        Assert.assertNotEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases ...", 4));
        Assert.assertNotEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases...", 4));
        Assert.assertNotEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases like... ", 4));
        Assert.assertNotEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases where...", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases like ...", 4));
        Assert.assertEquals(ServerParseShow.DATABASES, ServerParseShow.parse("show databases where ...", 4));
    }

    @Test
    public void testIsShowOther() {
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("show ...", 4));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("SHOW ...", 4));
        Assert.assertEquals(ServerParseShow.OTHER, ServerParseShow.parse("SHOW ... ", 4));
    }

    @Test
    public void testIsKill() {
        Assert.assertEquals(ServerParse.KILL, 0xff & serverParse.parse(" kill  ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & serverParse.parse("kill 111111 ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & serverParse.parse("KILL  1335505632"));
    }

    @Test
    public void testIsKillQuery() {
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & serverParse.parse(" kill query ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & serverParse.parse("kill   query 111111 ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & serverParse.parse("KILL QUERY 1335505632"));
    }

    @Test
    public void testIsSavepoint() {
        Assert.assertEquals(ServerParse.SAVEPOINT, serverParse.parse(" savepoint  ..."));
        Assert.assertEquals(ServerParse.SAVEPOINT, serverParse.parse("SAVEPOINT "));
        Assert.assertEquals(ServerParse.SAVEPOINT, serverParse.parse(" SAVEpoint   a"));
    }

    @Test
    public void testIsUse() {
        Assert.assertEquals(ServerParse.USE, 0xff & serverParse.parse(" use  ..."));
        Assert.assertEquals(ServerParse.USE, 0xff & serverParse.parse("USE "));
        Assert.assertEquals(ServerParse.USE, 0xff & serverParse.parse(" Use   a"));
    }

    @Test
    public void testIsStartTransaction() {
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse(" start transeeee "));
        Assert.assertEquals(ServerParse.OTHER, serverParse.parse(" start transaction  ..."));
        Assert.assertEquals(ServerParse.START_TRANSACTION, serverParse.parse("START TRANSACTION   "));
        Assert.assertEquals(ServerParse.START_TRANSACTION, serverParse.parse(" staRT   TRANSaction  "));
        Assert.assertEquals(ServerParse.START_TRANSACTION, serverParse.parse(" start transaction    /*!adfadfasdf*/  "));
        Assert.assertEquals(ServerParse.UNSUPPORT, serverParse.parse(" start transaction read   "));
        Assert.assertEquals(ServerParse.UNSUPPORT, serverParse.parse(" start transaction read  asdads  "));
        Assert.assertEquals(true, serverParse.parse(" start transaction readxasdads  ") != ServerParse.START_TRANSACTION);
        Assert.assertEquals(true, serverParse.parse(" start transactionread ") != ServerParse.START_TRANSACTION);
    }

    @Test
    public void testIsSelectVersionComment() {
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT,
                ServerParseSelect.parse(" select @@version_comment  ", 7));
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT, ServerParseSelect.parse("SELECT @@VERSION_COMMENT", 6));
        Assert.assertEquals(ServerParseSelect.VERSION_COMMENT,
                ServerParseSelect.parse(" selECT    @@VERSION_comment  ", 7));
    }

    @Test
    public void testIsSelectVersion() {
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse(" select version ()  ", 7));
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse("SELECT VERSION(  )", 6));
        Assert.assertEquals(ServerParseSelect.VERSION, ServerParseSelect.parse(" selECT    VERSION()  ", 7));
    }

    @Test
    public void testIsSelectDatabase() {
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse(" select database()  ", 7));
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse("SELECT DATABASE()", 7));
        Assert.assertEquals(ServerParseSelect.DATABASE, ServerParseSelect.parse(" selECT    DATABASE()  ", 7));
    }

    @Test
    public void testIsSelectUser() {
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse(" select user()  ", 7));
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse("SELECT USER()", 6));
        Assert.assertEquals(ServerParseSelect.USER, ServerParseSelect.parse(" selECT    USER()  ", 7));
    }

    @Test
    public void testIsSelectCurrentUser() {
        Assert.assertEquals(ServerParseSelect.CURRENT_USER, ServerParseSelect.parse(" select current_user()  ", 7));
        Assert.assertEquals(ServerParseSelect.CURRENT_USER, ServerParseSelect.parse("SELECT CURRENT_USER()", 6));
        Assert.assertEquals(ServerParseSelect.CURRENT_USER, ServerParseSelect.parse(" selECT    current_USER()  ", 7));
    }

    @Test
    public void testIdentity() {
        String stmt = "select @@identity";
        int indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterIdentity(stmt, stmt.indexOf('i'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select  /*foo*/@@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select/*foo*/ @@identitY  id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));
        stmt = "select/*foo*/ @@identitY As id";
        Assert.assertEquals(ServerParseSelect.IDENTITY, ServerParseSelect.parse(stmt, 6));

        stmt = "select  @@identity ,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as, ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity as id  , ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select  @@identity ass id   ";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));

    }

    @Test
    public void testLastInsertId() {
        String stmt = " last_insert_iD()";
        int indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD ()";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD ( /**/ )";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.length(), indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD (  )  ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  )";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = "last_iNsert_id(  ) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_iD";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_i     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_i    d ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id (     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  d)     ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(  ) d    ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(d)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\r\nd) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(-1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\n\r) ";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id (#\n\r)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);
        stmt = " last_insert_id(#\n\r)";
        indexAfterLastInsertIdFunc = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, stmt.indexOf('l'));
        Assert.assertEquals(stmt.lastIndexOf(')') + 1, indexAfterLastInsertIdFunc);

        stmt = "select last_insert_id(#\n\r)";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as id";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as `id`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as 'id'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  id";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  `id`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)  'id'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) a";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r) as";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) asd";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r) as 777";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        // NOTE: this should be invalid, ignore this bug
        stmt = "select last_insert_id(#\n\r)  777";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77``7`";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)ass";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a\\''";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a'''";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as 'a\"'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 6));
        stmt = "   select last_insert_id(#\n\r) As 'a\"'";
        Assert.assertEquals(ServerParseSelect.LAST_INSERT_ID, ServerParseSelect.parse(stmt, 9));

        stmt = "select last_insert_id(#\n\r)as 'a\"\\'";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77``7` ,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r)as `77`7`";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as,";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) ass a";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
        stmt = "select last_insert_id(#\n\r) as 'a";
        Assert.assertEquals(ServerParseSelect.OTHER, ServerParseSelect.parse(stmt, 6));
    }

    @Test
    public void testLockTable() {
        Assert.assertEquals(ServerParse.LOCK, serverParse.parse("lock tables ttt write;"));
        Assert.assertEquals(ServerParse.LOCK, serverParse.parse(" lock tables ttt read;"));
        Assert.assertEquals(ServerParse.LOCK, serverParse.parse("lock tables"));
    }

    @Test
    public void testUnlockTable() {
        Assert.assertEquals(ServerParse.UNLOCK, serverParse.parse("unlock tables"));
        Assert.assertEquals(ServerParse.UNLOCK, serverParse.parse(" unlock	 tables"));
    }


    @Test
    public void testCreateView() {
        Assert.assertEquals(ServerParse.CREATE_VIEW, serverParse.parse("create view asdfasdf as asdfasdfasdfsdf"));
        Assert.assertEquals(ServerParse.ALTER_VIEW, serverParse.parse("ALTER view x_xx_xx as select * from suntest"));
        Assert.assertEquals(ServerParse.REPLACE_VIEW, serverParse.parse("create or replace  view x_xx_xx as select * from suntest"));
        Assert.assertEquals(ServerParse.DROP_VIEW, serverParse.parse("DROP  view x_xx_xx"));
        Assert.assertEquals(ServerParse.DDL, serverParse.parse("create or replace viasdfasdfew asdfasdf as asdfasdfasdfsdf"));
        Assert.assertEquals(ServerParse.DDL, serverParse.parse("create   "));
    }

    @Test
    public void testDropPrepare() {
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, serverParse.parse("DROP PREPARE stmt_name"));
    }
}
