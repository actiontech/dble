/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.CustomMySQLHa;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.*;
import com.actiontech.dble.route.parser.ManagerParseShow;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.sqlengine.TransformSQLJob;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.util.Iterator;


/**
 * @author mycat
 */
public final class ShowHandler {
    private ShowHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseShow.SYSPARAM://add rainbow
                ShowSysParam.execute(c);
                break;
            case ManagerParseShow.SYSLOG: //add by zhuam
                ShowSysLog.execute(c, Integer.parseInt(stmt.substring(rs >>> 8).trim()));
                break;
            case ManagerParseShow.COMMAND:
                ShowCommand.execute(c);
                break;
            case ManagerParseShow.CONNECTION:
                ShowConnection.execute(c, stmt.substring(rs >>> 8).trim());
                break;
            case ManagerParseShow.BACKEND:
                ShowBackend.execute(c, stmt.substring(rs >>> 8).trim());
                break;
            case ManagerParseShow.BACKEND_OLD:
                ShowBackendOld.execute(c);
                break;
            case ManagerParseShow.BINLOG_STATUS:
                ShowBinlogStatus.execute(c);
                break;
            case ManagerParseShow.CONNECTION_SQL:
                ShowConnectionSQL.execute(c);
                break;
            case ManagerParseShow.DATABASE:
                ShowDatabase.execute(c);
                break;
            case ManagerParseShow.DATA_NODE:
                ShowDataNode.execute(c, null);
                break;
            case ManagerParseShow.DATANODE_SCHEMA: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataNode.execute(c, name);
                }
                break;
            }
            case ManagerParseShow.DATASOURCE:
                ShowDataSource.execute(c, null);
                break;
            case ManagerParseShow.DATASOURCE_WHERE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataSource.execute(c, name);
                }
                break;
            }
            case ManagerParseShow.TABLE_DATA_NODE: {
                String tableInfo = stmt.substring(rs >>> 8).trim();
                ShowTableDataNode.execute(c, tableInfo);
                break;
            }
            case ManagerParseShow.PAUSE_DATANDE:
                ShowPauseInfo.execute(c);
                break;
            case ManagerParseShow.HELP:
                ShowHelp.execute(c);
                break;
            case ManagerParseShow.SHOW_RELOAD:
                ShowReloadStatus.execute(c);
                break;
            case ManagerParseShow.HEARTBEAT:
                ShowHeartbeat.response(c);
                break;
            case ManagerParseShow.PROCESSOR:
                ShowProcessor.execute(c);
                break;
            case ManagerParseShow.PROCESS_LIST:
                ShowProcessList.execute(c);
                break;
            case ManagerParseShow.SERVER:
                ShowServer.execute(c);
                break;
            case ManagerParseShow.WHITE_HOST:
                ShowWhiteHost.execute(c);
                break;
            case ManagerParseShow.SQL:
                boolean isClearSql = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQL.execute(c, isClearSql);
                break;
            case ManagerParseShow.SQL_SLOW:
                boolean isClearSlow = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSlow.execute(c, isClearSlow);
                break;
            case ManagerParseShow.SQL_HIGH:
                boolean isClearHigh = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLHigh.execute(c, isClearHigh);
                break;
            case ManagerParseShow.SQL_LARGE:
                boolean isClearLarge = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLLarge.execute(c, isClearLarge);
                break;
            case ManagerParseShow.SQL_CONDITION:
                ShowSQLCondition.execute(c);
                break;
            case ManagerParseShow.SQL_RESULTSET:
                ShowSqlResultSet.execute(c);
                break;
            case ManagerParseShow.SQL_SUM_USER:
                boolean isClearSum = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSumUser.execute(c, isClearSum);
                break;
            case ManagerParseShow.SQL_SUM_TABLE:
                boolean isClearTable = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSumTable.execute(c, isClearTable);
                break;
            case ManagerParseShow.THREADPOOL:
                ShowThreadPool.execute(c);
                break;
            case ManagerParseShow.CACHE:
                ShowCache.execute(c);
                break;
            case ManagerParseShow.SESSION:
                ShowSession.execute(c);
                break;
            case ManagerParseShow.SESSION_XA:
                ShowXASession.execute(c);
                break;
            case ManagerParseShow.TIME_CURRENT:
                ShowTime.execute(c, System.currentTimeMillis());
                break;
            case ManagerParseShow.TIME_STARTUP:
                ShowTime.execute(c, DbleServer.getInstance().getStartupTime());
                break;
            case ManagerParseShow.VERSION:
                ShowVersion.execute(c);
                break;
            case ManagerParseShow.HEARTBEAT_DETAIL://by songwie
                ShowHeartbeatDetail.response(c, stmt);
                break;
            case ManagerParseShow.DATASOURCE_SYNC://by songwie
                ShowDatasourceSyn.response(c);
                break;
            case ManagerParseShow.DATASOURCE_SYNC_DETAIL://by songwie
                ShowDatasourceSynDetail.response(c, stmt);
                break;
            case ManagerParseShow.DIRECTMEMORY:
                ShowDirectMemory.execute(c);
                break;
            case ManagerParseShow.CONNECTION_COUNT:
                ShowConnectionCount.execute(c);
                break;
            case ManagerParseShow.COMMAND_COUNT:
                ShowCommandCount.execute(c);
                break;
            case ManagerParseShow.BACKEND_STAT:
                ShowBackendStat.execute(c);
                break;
            case ManagerParseShow.COST_TIME:
                ShowCostTimeStat.execute(c);
                break;
            case ManagerParseShow.THREAD_USED:
                ShowThreadUsed.execute(c);
                break;
            case ManagerParseShow.TABLE_ALGORITHM: {
                String tableInfo = stmt.substring(rs >>> 8).trim();
                ShowTableAlgorithm.execute(c, tableInfo);
                break;
            }
            case ManagerParseShow.SLOW_QUERY_LOG:
                ShowSingleValue.execute(c, "@@slow_query_log", SlowQueryLog.getInstance().isEnableSlowLog() ? 1L : 0L);
                break;
            case ManagerParseShow.SLOW_QUERY_TIME:
                ShowSingleValue.execute(c, "@@slow_query.time", SlowQueryLog.getInstance().getSlowTime());
                break;
            case ManagerParseShow.SLOW_QUERY_FLUSH_PERIOD:
                ShowSingleValue.execute(c, "@@slow_query.flushperiod", SlowQueryLog.getInstance().getFlushPeriod());
                break;
            case ManagerParseShow.SLOW_QUERY_FLUSH_SIZE:
                ShowSingleValue.execute(c, "@@slow_query.flushsize", SlowQueryLog.getInstance().getFlushSize());
                break;
            case ManagerParseShow.ALERT:
                ShowSingleValue.execute(c, "@@alert", AlertUtil.isEnable() ? 1L : 0L);
                break;
            case ManagerParseShow.CUSTOM_MYSQL_HA:
                ShowSingleValue.execute(c, "@@custom_mysql_ha", CustomMySQLHa.getInstance().isProcessAlive() ? 1L : 0L);
                break;
            case ManagerParseShow.COLLATION:
                ShowCollatin.execute(c);
                break;
            case ManagerParseShow.DDL_STATE:
                ShowDdlState.execute(c);
                break;
            case ManagerParseShow.SHOW_USER:
                ShowUser.execute(c);
                break;
            case ManagerParseShow.SHOW_USER_PRIVILEGE:
                ShowUserPrivilege.execute(c);
                break;
            case ManagerParseShow.SHOW_QUESTIONS:
                ShowQuestions.execute(c);
                break;
            case ManagerParseShow.DATADISTRIBUTION_WHERE:
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataDistribution.execute(c, name);
                }
                break;
            case ManagerParseShow.CONNECTION_SQL_STATUS:
                String id = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(id)) {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowConnectionSQLStatus.execute(c, id);
                }
                break;
            default:
                if (isSupportShow(stmt)) {
                    Iterator<PhysicalDataHost> iterator = DbleServer.getInstance().getConfig().getDataHosts().values().iterator();
                    if (iterator.hasNext()) {
                        PhysicalDataHost pool = iterator.next();
                        final PhysicalDataSource source = pool.getWriteSource();
                        TransformSQLJob sqlJob = new TransformSQLJob(stmt, null, source, c);
                        sqlJob.run();
                    } else {
                        c.writeErrMessage(ErrorCode.ER_YES, "no valid data host");
                    }
                } else {
                    c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                }
        }
    }

    private static boolean isSupportShow(String stmt) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        SQLStatement statement = parser.parseStatement();
        return statement instanceof MySqlShowWarningsStatement || statement instanceof MySqlShowVariantsStatement;
    }
}
