/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.parser;

import com.actiontech.dble.route.parser.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat
 */
public class ManagerParserTest {


    @Test
    public void testIsShow() {
        Assert.assertEquals(ManagerParse.SHOW, 0xff & ManagerParse.parse("show databases"));
        Assert.assertEquals(ManagerParse.SHOW, 0xff & ManagerParse.parse("SHOW DATABASES"));
        Assert.assertEquals(ManagerParse.SHOW, 0xff & ManagerParse.parse("SHOW databases"));
    }

    @Test
    public void testShowCommand() {
        Assert.assertEquals(ManagerParseShow.COMMAND, ManagerParseShow.parse("show @@command", 5));
        Assert.assertEquals(ManagerParseShow.COMMAND, ManagerParseShow.parse("SHOW @@COMMAND", 5));
        Assert.assertEquals(ManagerParseShow.COMMAND, ManagerParseShow.parse("show @@COMMAND", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@COMMAND ASDSFASDFASDF", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@COMMANDASDSFASDFASDF", 5));
    }

    @Test
    public void testShowConnection() {
        Assert.assertEquals(ManagerParseShow.CONNECTION, ManagerParseShow.parse("show @@connection", 5));
        Assert.assertEquals(ManagerParseShow.CONNECTION, ManagerParseShow.parse("SHOW @@CONNECTION", 5));
        Assert.assertEquals(ManagerParseShow.CONNECTION, ManagerParseShow.parse("show @@CONNECTION", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@CONNECTIONADFASDF", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@CONNECTION ADFASDF", 5));
    }

    @Test
    public void testShowConnectionSQL() {
        Assert.assertEquals(ManagerParseShow.CONNECTION_SQL, ManagerParseShow.parse("show @@connection.sql", 5));
        Assert.assertEquals(ManagerParseShow.CONNECTION_SQL, ManagerParseShow.parse("SHOW @@CONNECTION.SQL", 5));
        Assert.assertEquals(ManagerParseShow.CONNECTION_SQL, ManagerParseShow.parse("show @@CONNECTION.Sql", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@CONNECTION.Sql ASDFASDF", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@CONNECTION.SqlASDFASDF", 5));
    }

    @Test
    public void testShowDatabase() {
        Assert.assertEquals(ManagerParseShow.DATABASE, ManagerParseShow.parse("show @@database", 5));
        Assert.assertEquals(ManagerParseShow.DATABASE, ManagerParseShow.parse("SHOW @@DATABASE", 5));
        Assert.assertEquals(ManagerParseShow.DATABASE, ManagerParseShow.parse("show @@DATABASE", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@DATABASEADSFASDF", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@DATABASE ASDFASDDF", 5));
    }

    @Test
    public void testShowDataNode() {
        Assert.assertEquals(ManagerParseShow.DATA_NODE, ManagerParseShow.parse("show @@datanode", 5));
        Assert.assertEquals(ManagerParseShow.DATA_NODE, ManagerParseShow.parse("SHOW @@DATANODE", 5));
        Assert.assertEquals(ManagerParseShow.DATA_NODE, ManagerParseShow.parse("show @@DATANODE", 5));
        Assert.assertEquals(ManagerParseShow.DATA_NODE, ManagerParseShow.parse("show @@DATANODE   ", 5));
        Assert.assertEquals(ManagerParseShow.DATANODE_WHERE,
                0xff & ManagerParseShow.parse("show @@DATANODE WHERE SCHEMA=1", 5));
        Assert.assertEquals(ManagerParseShow.DATANODE_WHERE,
                0xff & ManagerParseShow.parse("show @@DATANODE WHERE schema =1", 5));
        Assert.assertEquals(ManagerParseShow.DATANODE_WHERE,
                0xff & ManagerParseShow.parse("show @@DATANODE WHERE SCHEMA= 1", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse("show @@DATANODEWHERE SCHEMA= 1", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse("show @@DATANODE WHERESCHEMA=1", 5));

    }

    @Test
    public void testShowDataSource() {
        Assert.assertEquals(ManagerParseShow.DATASOURCE, ManagerParseShow.parse("show @@datasource", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE, ManagerParseShow.parse("SHOW @@DATASOURCE", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE, ManagerParseShow.parse(" show  @@DATASOURCE ", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE, ManagerParseShow.parse(" show  @@DATASOURCE   ", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE_WHERE,
                0xff & ManagerParseShow.parse(" show  @@DATASOURCE where datanode = 1", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE_WHERE,
                0xff & ManagerParseShow.parse(" show  @@DATASOURCE where datanode=1", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE_WHERE,
                0xff & ManagerParseShow.parse(" show  @@DATASOURCE WHERE datanode = 1", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE_WHERE,
                0xff & ManagerParseShow.parse(" show  @@DATASOURCE where DATAnode= 1 ", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse(" show  @@DATASOURCEwhere DATAnode= 1 ", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse(" show  @@DATASOURCE whereDATAnode= 1 ", 5));
    }

    @Test
    public void testShowHelp() {
        Assert.assertEquals(ManagerParseShow.HELP, ManagerParseShow.parse("show @@help", 5));
        Assert.assertEquals(ManagerParseShow.HELP, ManagerParseShow.parse("SHOW @@HELP", 5));
        Assert.assertEquals(ManagerParseShow.HELP, ManagerParseShow.parse("show @@HELP", 5));
    }

    @Test
    public void testShowHeartbeat() {
        Assert.assertEquals(ManagerParseShow.HEARTBEAT, ManagerParseShow.parse("show @@heartbeat", 5));
        Assert.assertEquals(ManagerParseShow.HEARTBEAT, ManagerParseShow.parse("SHOW @@hearTBeat ", 5));
        Assert.assertEquals(ManagerParseShow.HEARTBEAT, ManagerParseShow.parse("  show   @@HEARTBEAT  ", 6));
    }

    @Test
    public void testShowProcessor() {
        Assert.assertEquals(ManagerParseShow.PROCESSOR, ManagerParseShow.parse("show @@processor", 5));
        Assert.assertEquals(ManagerParseShow.PROCESSOR, ManagerParseShow.parse("SHOW @@PROCESSOR", 5));
        Assert.assertEquals(ManagerParseShow.PROCESSOR, ManagerParseShow.parse("show @@PROCESSOR", 5));
    }

    @Test
    public void testShowServer() {
        Assert.assertEquals(ManagerParseShow.SERVER, ManagerParseShow.parse("show @@server", 5));
        Assert.assertEquals(ManagerParseShow.SERVER, ManagerParseShow.parse("SHOW @@SERVER", 5));
        Assert.assertEquals(ManagerParseShow.SERVER, ManagerParseShow.parse("show @@SERVER", 5));
    }

    @Test
    public void testShowThreadPool() {
        Assert.assertEquals(ManagerParseShow.THREADPOOL, ManagerParseShow.parse("show @@threadPool", 5));
        Assert.assertEquals(ManagerParseShow.THREADPOOL, ManagerParseShow.parse("SHOW @@THREADPOOL", 5));
        Assert.assertEquals(ManagerParseShow.THREADPOOL, ManagerParseShow.parse("show @@THREADPOOL", 5));
    }

    @Test
    public void testShowBackend() {
        Assert.assertEquals(ManagerParseShow.BACKEND, ManagerParseShow.parse("show @@backend", 5));
        Assert.assertEquals(ManagerParseShow.BACKEND, ManagerParseShow.parse("SHOW @@BACkend", 5));
        Assert.assertEquals(ManagerParseShow.BACKEND, ManagerParseShow.parse("show @@BACKEND ", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@backendASDFASDF", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@backend ASDFASDF", 5));
    }

    @Test
    public void testShowTimeCurrent() {
        Assert.assertEquals(ManagerParseShow.TIME_CURRENT, ManagerParseShow.parse("show @@time.current", 5));
        Assert.assertEquals(ManagerParseShow.TIME_CURRENT, ManagerParseShow.parse("SHOW @@TIME.CURRENT", 5));
        Assert.assertEquals(ManagerParseShow.TIME_CURRENT, ManagerParseShow.parse("show @@TIME.current", 5));
    }

    @Test
    public void testShowTimeStartUp() {
        Assert.assertEquals(ManagerParseShow.TIME_STARTUP, ManagerParseShow.parse("show @@time.startup", 5));
        Assert.assertEquals(ManagerParseShow.TIME_STARTUP, ManagerParseShow.parse("SHOW @@TIME.STARTUP", 5));
        Assert.assertEquals(ManagerParseShow.TIME_STARTUP, ManagerParseShow.parse("show @@TIME.startup", 5));
    }

    @Test
    public void testShowVersion() {
        Assert.assertEquals(ManagerParseShow.VERSION, ManagerParseShow.parse("show @@version", 5));
        Assert.assertEquals(ManagerParseShow.VERSION, ManagerParseShow.parse("SHOW @@VERSION", 5));
        Assert.assertEquals(ManagerParseShow.VERSION, ManagerParseShow.parse("show @@VERSION", 5));
    }

    @Test
    public void testShowSQL() {
        Assert.assertEquals(ManagerParseShow.SQL, ManagerParseShow.parse("show @@sql where id = -1079800749", 5));
        Assert.assertEquals(ManagerParseShow.SQL, ManagerParseShow.parse("SHOW @@SQL WHERE ID = -1079800749", 5));
        Assert.assertEquals(ManagerParseShow.SQL, ManagerParseShow.parse("show @@Sql WHERE ID = -1079800749", 5));
        Assert.assertEquals(ManagerParseShow.SQL, ManagerParseShow.parse("show @@sql where id=-1079800749", 5));
        Assert.assertEquals(ManagerParseShow.SQL, ManagerParseShow.parse("show @@sql where id   =-1079800749 ", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@sql where id   :-1079800749 ", 5));
        Assert.assertEquals(ManagerParseShow.OTHER, ManagerParseShow.parse("show @@sql whereid   =-1079800749 ", 5));
    }

    @Test
    public void testShowSQLSlow() {
        Assert.assertEquals(ManagerParseShow.SQL_SLOW, ManagerParseShow.parse("show @@sql.slow", 5));
        Assert.assertEquals(ManagerParseShow.SQL_SLOW, ManagerParseShow.parse("SHOW @@SQL.SLOW", 5));
        Assert.assertEquals(ManagerParseShow.SQL_SLOW, ManagerParseShow.parse("SHOW @@sql.slow", 5));
    }

    @Test
    public void testSwitchPool() {
        Assert.assertEquals(ManagerParse.SWITCH, 0xff & ManagerParse.parse("switch @@pool offer2$0-2"));
        Assert.assertEquals(ManagerParse.SWITCH, 0xff & ManagerParse.parse("SWITCH @@POOL offer2$0-2"));
        Assert.assertEquals(ManagerParse.SWITCH, 0xff & ManagerParse.parse("switch @@pool offer2$0-2 :2"));
    }

    @Test
    public void testComment() {
        Assert.assertEquals(ManagerParse.SWITCH, 0xff & ManagerParse.parse("/* abc */switch @@pool offer2$0-2"));
        Assert.assertEquals(ManagerParse.SHOW, 0xff & ManagerParse.parse(" /** 111**/Show @@help"));
    }

    @Test
    public void testStop() {
        Assert.assertEquals(ManagerParse.STOP, 0xff & ManagerParse.parse("stop @@"));
        Assert.assertEquals(ManagerParse.STOP, 0xff & ManagerParse.parse(" STOP "));
    }

    @Test
    public void testStopHeartBeat() {
        Assert.assertEquals(ManagerParseStop.HEARTBEAT, ManagerParseStop.parse("stop @@heartbeat ds:1000", 4));
        Assert.assertEquals(ManagerParseStop.HEARTBEAT, ManagerParseStop.parse(" STOP  @@HEARTBEAT ds:1000", 5));
        Assert.assertEquals(ManagerParseStop.HEARTBEAT, ManagerParseStop.parse(" STOP  @@heartbeat ds:1000", 5));
    }

    @Test
    public void testReload() {
        Assert.assertEquals(ManagerParse.RELOAD, 0xff & ManagerParse.parse("reload @@"));
        Assert.assertEquals(ManagerParse.RELOAD, 0xff & ManagerParse.parse(" RELOAD "));
    }

    @Test
    public void testReloadConfig() {
        Assert.assertEquals(ManagerParseReload.CONFIG, ManagerParseReload.parse("reload @@config", 7));
        Assert.assertEquals(ManagerParseReload.CONFIG, ManagerParseReload.parse(" RELOAD  @@CONFIG ", 7));
        Assert.assertEquals(ManagerParseReload.CONFIG, ManagerParseReload.parse(" RELOAD  @@config ", 7));
    }


    @Test
    public void testRollback() {
        Assert.assertEquals(ManagerParse.ROLLBACK, 0xff & ManagerParse.parse("rollback @@"));
        Assert.assertEquals(ManagerParse.ROLLBACK, 0xff & ManagerParse.parse(" ROLLBACK "));
    }

    @Test
    public void testOnOff() {
        Assert.assertEquals(ManagerParse.ONLINE, ManagerParse.parse("online "));
        Assert.assertEquals(ManagerParse.ONLINE, ManagerParse.parse(" Online"));
        Assert.assertEquals(ManagerParse.OTHER, ManagerParse.parse(" Online2"));
        Assert.assertEquals(ManagerParse.OTHER, ManagerParse.parse("Online2 "));
        Assert.assertEquals(ManagerParse.OFFLINE, ManagerParse.parse(" Offline"));
        Assert.assertEquals(ManagerParse.OFFLINE, ManagerParse.parse("offLine\t"));
        Assert.assertEquals(ManagerParse.OTHER, ManagerParse.parse("onLin"));
        Assert.assertEquals(ManagerParse.OTHER, ManagerParse.parse(" onlin"));
    }

    @Test
    public void testRollbackConfig() {
        Assert.assertEquals(ManagerParseRollback.CONFIG, ManagerParseRollback.parse("rollback @@config", 8));
        Assert.assertEquals(ManagerParseRollback.CONFIG, ManagerParseRollback.parse(" ROLLBACK  @@CONFIG ", 9));
        Assert.assertEquals(ManagerParseRollback.CONFIG, ManagerParseRollback.parse(" ROLLBACK  @@config ", 9));
    }


    @Test
    public void testGetWhere() {
        Assert.assertEquals("123", ManagerParseShow.getWhereParameter("where id = 123"));
        Assert.assertEquals("datanode", ManagerParseShow.getWhereParameter("where datanode =    datanode"));
        Assert.assertEquals("schema", ManagerParseShow.getWhereParameter("where schema =schema   "));
    }

    @Test
    public void testclearSlowSchema() {
        Assert.assertEquals(ManagerParseClear.SLOW_SCHEMA,
                0xff & ManagerParseClear.parse("clear @@slow where schema=s", 5));
        Assert.assertEquals(ManagerParseClear.SLOW_SCHEMA,
                0xff & ManagerParseClear.parse("CLEAR @@SLOW WHERE SCHEMA= S", 5));
        Assert.assertEquals(ManagerParseClear.SLOW_SCHEMA,
                0xff & ManagerParseClear.parse("CLEAR @@slow where SCHEMA= s", 5));
    }

    @Test
    public void testclearSlowDataNode() {
        Assert.assertEquals(ManagerParseClear.SLOW_DATANODE,
                0xff & ManagerParseClear.parse("clear @@slow where datanode=d", 5));
        Assert.assertEquals(ManagerParseClear.SLOW_DATANODE,
                0xff & ManagerParseClear.parse("CLEAR @@SLOW WHERE DATANODE= D", 5));
        Assert.assertEquals(ManagerParseClear.SLOW_DATANODE,
                0xff & ManagerParseClear.parse("clear @@SLOW where  DATANODE= d", 5));
    }

    @Test
    public void testHeartBearDetail() {
        Assert.assertEquals(ManagerParseShow.HEARTBEAT_DETAIL,
                0xff & ManagerParseShow.parse("show @@heartbeat.detail where name=master", 5));
    }

    @Test
    public void testSynStatus() {
        Assert.assertEquals(ManagerParseShow.DATASOURCE_SYNC,
                0xff & ManagerParseShow.parse("show @@datasource.synstatus", 5));
    }

    @Test
    public void testSynDetail() {
        Assert.assertEquals(ManagerParseShow.DATASOURCE_SYNC_DETAIL,
                ManagerParseShow.parse("show @@datasource.syndetail where name=slave", 5));
        Assert.assertEquals(ManagerParseShow.DATASOURCE_SYNC_DETAIL,
                ManagerParseShow.parse("show @@datasource.syndetail       where    name =   slave", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse("show @@datasource.syndetailwhere    name =   slave", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse("show @@datasource.syndetail wherename=slave", 5));
        Assert.assertEquals(ManagerParseShow.OTHER,
                ManagerParseShow.parse("show @@datasource.syndetail where name=slave ASDFASDF", 5));
    }

}