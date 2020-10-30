/*
 * Copyright (C) 2016-2019 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.parser;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.parser.ServerParseSelect;
import com.actiontech.dble.server.parser.ServerParseShow;
import com.actiontech.dble.server.parser.ServerParseStart;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author mycat
 */
public class ServerParserTest {

    @Test
    public void testIsBegin() {
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("begin"));
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("BEGIN"));
        Assert.assertEquals(ServerParse.BEGIN, ServerParse.parse("BegIn"));
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse("BegIn X"));
    }

    @Test
    public void testIsCommit() {
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("commit"));
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("COMMIT"));
        Assert.assertEquals(ServerParse.COMMIT, ServerParse.parse("cOmmiT "));
    }


    @Test
    public void testComment() {
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_CMD_COMMENT, ServerParse.parse("/*!40101 SET @saved_cs_client     = @@character_set_client */"));

        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("/*SET @saved_cs_client     = @@character_set_client */"));
    }

    @Test
    public void testHintComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("/*#dble:sharding=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & ServerParse.parse("/*#dble: sharding = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & ServerParse.parse("/*#dble: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testOldHintComment() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("/*!dble:sharding=DN1*/SELECT ..."));
        Assert.assertEquals(ServerParse.UPDATE, 0xff & ServerParse.parse("/*!dble: sharding = DN1 */ UPDATE ..."));
        Assert.assertEquals(ServerParse.DELETE, 0xff & ServerParse.parse("/*!dble: sql = SELECT id FROM user */ DELETE ..."));
    }

    @Test
    public void testDoubleDashComment() {
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("--     "));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("--\t    "));
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse("- \n"));
        Assert.assertEquals(ServerParse.MYSQL_COMMENT, ServerParse.parse("--         select * from test"));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("-- select *\n-- fdfadsfad\ninsert into test values(1)"));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("--\tselect *\n-- fdfadsfad\ninsert into test values(1)"));
        Assert.assertEquals(ServerParse.OTHER, ServerParse.parse("-- select *\nfdfadsfad\ninsert into test values(1)"));
    }

    @Test
    public void testIsDelete() {
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("delete ..."));
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("DELETE ..."));
        Assert.assertEquals(ServerParse.DELETE, ServerParse.parse("DeletE ..."));
    }

    @Test
    public void testIsInsert() {
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("insert ..."));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("INSERT ..."));
        Assert.assertEquals(ServerParse.INSERT, ServerParse.parse("InserT ..."));
    }

    @Test
    public void testIsReplace() {
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("replace ..."));
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("REPLACE ..."));
        Assert.assertEquals(ServerParse.REPLACE, ServerParse.parse("rEPLACe ..."));
    }

    @Test
    public void testIsRollback() {
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("rollback"));
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("rollback  work   /* dble_dest_expect:M */"));
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("ROLLBACK"));
        Assert.assertEquals(ServerParse.ROLLBACK, ServerParse.parse("rolLBACK "));
    }

    @Test
    public void testIsSelect() {
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("select ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("SELECT ..."));
        Assert.assertEquals(ServerParse.SELECT, 0xff & ServerParse.parse("sELECt ..."));
    }

    @Test
    public void testIsSet() {
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("set ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("SET ..."));
        Assert.assertEquals(ServerParse.SET, 0xff & ServerParse.parse("sEt ..."));
    }

    @Test
    public void testIsShow() {
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("show ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("SHOW ..."));
        Assert.assertEquals(ServerParse.SHOW, 0xff & ServerParse.parse("sHOw ..."));
    }

    @Test
    public void testIsStart() {
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("start ..."));
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("START ..."));
        Assert.assertEquals(ServerParse.START, 0xff & ServerParse.parse("stART ..."));
    }

    @Test
    public void testIsUpdate() {
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("update ..."));
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("UPDATE ..."));
        Assert.assertEquals(ServerParse.UPDATE, ServerParse.parse("UPDate ..."));
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
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse(" kill  ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse("kill 111111 ..."));
        Assert.assertEquals(ServerParse.KILL, 0xff & ServerParse.parse("KILL  1335505632"));
    }

    @Test
    public void testIsKillQuery() {
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse(" kill query ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse("kill   query 111111 ..."));
        Assert.assertEquals(ServerParse.KILL_QUERY, 0xff & ServerParse.parse("KILL QUERY 1335505632"));
    }

    @Test
    public void testIsSavepoint() {
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse(" savepoint  ..."));
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse("SAVEPOINT "));
        Assert.assertEquals(ServerParse.SAVEPOINT, ServerParse.parse(" SAVEpoint   a"));
    }

    @Test
    public void testIsUse() {
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse(" use  ..."));
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse("USE "));
        Assert.assertEquals(ServerParse.USE, 0xff & ServerParse.parse(" Use   a"));
    }

    @Ignore
    @Test
    public void testIsStartTransaction() {
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse(" start transaction  ...", 6));
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse("START TRANSACTION", 5));
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse(" staRT   TRANSaction  ", 6));
        Assert.assertEquals(ServerParseStart.TRANSACTION, ServerParseStart.parse(" start transaction /*!adfadfasdf*/  ", 6));
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
        Assert.assertEquals(ServerParse.LOCK, ServerParse.parse("lock tables ttt write;"));
        Assert.assertEquals(ServerParse.LOCK, ServerParse.parse(" lock tables ttt read;"));
        Assert.assertEquals(ServerParse.LOCK, ServerParse.parse("lock tables"));
    }

    @Test
    public void testUnlockTable() {
        Assert.assertEquals(ServerParse.UNLOCK, ServerParse.parse("unlock tables"));
        Assert.assertEquals(ServerParse.UNLOCK, ServerParse.parse(" unlock	 tables"));
    }


    @Test
    public void testCreateView() {
        Assert.assertEquals(ServerParse.CREATE_VIEW, ServerParse.parse("create view asdfasdf as asdfasdfasdfsdf"));
        Assert.assertEquals(ServerParse.ALTER_VIEW, ServerParse.parse("ALTER view x_xx_xx as select * from suntest"));
        Assert.assertEquals(ServerParse.REPLACE_VIEW, ServerParse.parse("create or replace  view x_xx_xx as select * from suntest"));
        Assert.assertEquals(ServerParse.DROP_VIEW, ServerParse.parse("DROP  view x_xx_xx"));
        Assert.assertEquals(ServerParse.DDL, ServerParse.parse("create or replace viasdfasdfew asdfasdf as asdfasdfasdfsdf"));
        Assert.assertEquals(ServerParse.DDL, ServerParse.parse("create   "));
    }

    @Test
    public void testDropPrepare() {
        Assert.assertEquals(ServerParse.SCRIPT_PREPARE, ServerParse.parse("DROP PREPARE stmt_name"));
    }
}
