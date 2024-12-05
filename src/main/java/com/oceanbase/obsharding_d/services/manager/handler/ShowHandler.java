/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.ManagerParseShow;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.*;
import com.oceanbase.obsharding_d.singleton.CapClientFoundRows;
import com.oceanbase.obsharding_d.singleton.CustomMySQLHa;
import com.oceanbase.obsharding_d.sqlengine.TransformSQLJob;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


/**
 * @author mycat
 */
public final class ShowHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowHandler.class);

    private ShowHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseShow.SYSPARAM://add rainbow
                ShowSysParam.execute(service);
                break;
            case ManagerParseShow.COMMAND:
                ShowCommand.execute(service);
                break;
            case ManagerParseShow.CONNECTION:
                ShowConnection.execute(service, stmt.substring(rs >>> 8).trim());
                break;
            case ManagerParseShow.CONNECTION_POOL_PROPERTY:
                ShowConnectionPoolProperty.execute(service);
                break;
            case ManagerParseShow.BACKEND:
                ShowBackend.execute(service, stmt.substring(rs >>> 8).trim());
                break;
            case ManagerParseShow.BACKEND_OLD:
                ShowBackendOld.execute(service);
                break;
            case ManagerParseShow.BINLOG_STATUS:
                ShowBinlogStatus.execute(service);
                break;
            case ManagerParseShow.CONNECTION_SQL:
                ShowConnectionSQL.execute(service);
                break;
            case ManagerParseShow.DATABASE:
                ShowDatabase.execute(service);
                break;
            case ManagerParseShow.DATABASES:
                ShowDatabases.execute(service);
                break;
            case ManagerParseShow.SHARDING_NODE:
                ShowShardingNode.execute(service, null);
                break;
            case ManagerParseShow.SHARDING_NODE_SCHEMA: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowShardingNode.execute(service, name);
                }
                break;
            }
            case ManagerParseShow.DB_INSTANCE:
                ShowDbInstance.execute(service, null);
                break;
            case ManagerParseShow.DB_INSTANCE_WHERE: {
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDbInstance.execute(service, name);
                }
                break;
            }
            case ManagerParseShow.TABLE_SHARDING_NODE: {
                String tableInfo = stmt.substring(rs >>> 8).trim();
                ShowTableShardingNode.execute(service, tableInfo);
                break;
            }
            case ManagerParseShow.PAUSE_DATANDE:
                ShowPauseInfo.execute(service);
                break;
            case ManagerParseShow.HELP:
                ShowHelp.execute(service);
                break;
            case ManagerParseShow.SHOW_RELOAD:
                ShowReloadStatus.execute(service);
                break;
            case ManagerParseShow.HEARTBEAT:
                ShowHeartbeat.response(service);
                break;
            case ManagerParseShow.PROCESSOR:
                ShowProcessor.execute(service);
                break;
            case ManagerParseShow.PROCESS_LIST:
                ShowProcessList.execute(service);
                break;
            case ManagerParseShow.SERVER:
                ShowServer.execute(service);
                break;
            case ManagerParseShow.WHITE_HOST:
                ShowWhiteHost.execute(service);
                break;
            case ManagerParseShow.TABLES:
                ShowTables.execute(service, stmt);
                break;
            case ManagerParseShow.SQL:
                boolean isClearSql = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQL.execute(service, isClearSql);
                break;
            case ManagerParseShow.SQL_SLOW:
                boolean isClearSlow = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSlow.execute(service, isClearSlow);
                break;
            case ManagerParseShow.GENERAL_LOG:
                GeneralLogCf.ShowGeneralLog.execute(service);
                break;
            case ManagerParseShow.SQL_HIGH:
                boolean isClearHigh = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLHigh.execute(service, isClearHigh);
                break;
            case ManagerParseShow.SQL_LARGE:
                boolean isClearLarge = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLLarge.execute(service, isClearLarge);
                break;
            case ManagerParseShow.SQL_CONDITION:
                ShowSQLCondition.execute(service);
                break;
            case ManagerParseShow.SQL_RESULTSET:
                ShowSqlResultSet.execute(service);
                break;
            case ManagerParseShow.SQL_SUM_USER:
                boolean isClearSum = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSumUser.execute(service, isClearSum);
                break;
            case ManagerParseShow.SQL_SUM_TABLE:
                boolean isClearTable = Boolean.parseBoolean(stmt.substring(rs >>> 8).trim());
                ShowSQLSumTable.execute(service, isClearTable);
                break;
            case ManagerParseShow.THREADPOOL:
                ShowThreadPool.execute(service);
                break;
            case ManagerParseShow.THREAD_POOL_TASK:
                ShowThreadPoolTask.execute(service);
                break;
            case ManagerParseShow.CACHE:
                ShowCache.execute(service);
                break;
            case ManagerParseShow.SESSION:
                ShowSession.execute(service);
                break;
            case ManagerParseShow.SESSION_XA:
                ShowXASession.execute(service);
                break;
            case ManagerParseShow.TIME_CURRENT:
                ShowTime.execute(service, System.currentTimeMillis());
                break;
            case ManagerParseShow.TIME_STARTUP:
                ShowTime.execute(service, OBsharding_DServer.getInstance().getStartupTime());
                break;
            case ManagerParseShow.VERSION:
                ShowVersion.execute(service);
                break;
            case ManagerParseShow.HEARTBEAT_DETAIL://by songwie
                ShowHeartbeatDetail.response(service, stmt);
                break;
            case ManagerParseShow.DB_INSTANCE_SYNC://by songwie
                ShowDbInstanceSyn.response(service);
                break;
            case ManagerParseShow.DB_INSTANCE_SYNC_DETAIL://by songwie
                ShowDbInstanceSynDetail.response(service, stmt);
                break;
            case ManagerParseShow.DIRECTMEMORY:
                ShowDirectMemory.execute(service);
                break;
            case ManagerParseShow.CONNECTION_COUNT:
                ShowConnectionCount.execute(service);
                break;
            case ManagerParseShow.COMMAND_COUNT:
                ShowCommandCount.execute(service);
                break;
            case ManagerParseShow.BACKEND_STAT:
                ShowBackendStat.execute(service);
                break;
            case ManagerParseShow.COST_TIME:
                ShowCostTimeStat.execute(service);
                break;
            case ManagerParseShow.THREAD_USED:
                ShowThreadUsed.execute(service);
                break;
            case ManagerParseShow.TABLE_ALGORITHM: {
                String tableInfo = stmt.substring(rs >>> 8).trim();
                ShowTableAlgorithm.execute(service, tableInfo);
                break;
            }
            case ManagerParseShow.SLOW_QUERY_LOG:
                ShowSingleValue.execute(service, "@@slow_query_log", SlowQueryLog.getInstance().isEnableSlowLog() ? 1L : 0L);
                break;
            case ManagerParseShow.SLOW_QUERY_TIME:
                ShowSingleValue.execute(service, "@@slow_query.time", SlowQueryLog.getInstance().getSlowTime());
                break;
            case ManagerParseShow.SLOW_QUERY_FLUSH_PERIOD:
                ShowSingleValue.execute(service, "@@slow_query.flushperiod", SlowQueryLog.getInstance().getFlushPeriod());
                break;
            case ManagerParseShow.SLOW_QUERY_FLUSH_SIZE:
                ShowSingleValue.execute(service, "@@slow_query.flushsize", SlowQueryLog.getInstance().getFlushSize());
                break;
            case ManagerParseShow.ALERT:
                ShowSingleValue.execute(service, "@@alert", AlertUtil.isEnable() ? 1L : 0L);
                break;
            case ManagerParseShow.CUSTOM_MYSQL_HA:
                ShowSingleValue.execute(service, "@@custom_mysql_ha", CustomMySQLHa.getInstance().isProcessAlive() ? 1L : 0L);
                break;
            case ManagerParseShow.CAP_CLIENT_FOUND_ROWS:

                ShowSingleValue.execute(service, "@@cap_client_found_rows", CapClientFoundRows.getInstance().isEnableCapClientFoundRows() ? 1L : 0L);
                break;
            case ManagerParseShow.COLLATION:
                ShowCollatin.execute(service);
                break;
            case ManagerParseShow.DDL_STATE:
                ShowDdlState.execute(service);
                break;
            case ManagerParseShow.SHOW_USER:
                ShowUser.execute(service);
                break;
            case ManagerParseShow.SHOW_USER_PRIVILEGE:
                ShowUserPrivilege.execute(service);
                break;
            case ManagerParseShow.SHOW_QUESTIONS:
                ShowQuestions.execute(service);
                break;
            case ManagerParseShow.DATADISTRIBUTION_WHERE:
                String name = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(name)) {
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowDataDistribution.execute(service, name);
                }
                break;
            case ManagerParseShow.CONNECTION_SQL_STATUS:
                String id = stmt.substring(rs >>> 8).trim();
                if (StringUtil.isEmpty(id)) {
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                } else {
                    ShowConnectionSQLStatus.execute(service, id);
                }
                break;
            case ManagerParseShow.STATISTIC:
                StatisticCf.Show.execute(service);
                break;
            case ManagerParseShow.STATISTIC_QUEUE_USAGE:
                StatisticCf.Queue.show(service);
                break;
            case ManagerParseShow.LOAD_DATA_FAIL:
                ShowLoadDataErrorFile.execute(service);
                break;
            default:
                if (isSupportShow(stmt)) {
                    Iterator<PhysicalDbGroup> iterator = OBsharding_DServer.getInstance().getConfig().getDbGroups().values().iterator();
                    if (iterator.hasNext()) {
                        PhysicalDbGroup pool = iterator.next();
                        final PhysicalDbInstance source = pool.getWriteDbInstance();
                        TransformSQLJob sqlJob = new TransformSQLJob(stmt, null, source, service);
                        sqlJob.run();
                    } else {
                        service.writeErrMessage(ErrorCode.ER_YES, "no valid dbGroup");
                    }
                } else {
                    LOGGER.warn("Unsupported show:" + stmt);
                    service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
                }
        }
    }

    private static boolean isSupportShow(String stmt) {
        SQLStatementParser parser = new MySqlStatementParser(stmt);
        SQLStatement statement;
        try {
            statement = parser.parseStatement();
        } catch (ParserException e) {
            return false;
        }
        return statement instanceof MySqlShowWarningsStatement || statement instanceof MySqlShowVariantsStatement;
    }
}
