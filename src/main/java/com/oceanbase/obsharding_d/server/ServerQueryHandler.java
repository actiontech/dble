/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.handler.FrontendQueryHandler;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;
import com.oceanbase.obsharding_d.server.handler.*;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.server.parser.ShardingServerParse;
import com.oceanbase.obsharding_d.services.TransactionOperate;
import com.oceanbase.obsharding_d.util.exception.NeedDelayedException;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class ServerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final ShardingService service;
    private Boolean readOnly = true;

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            if (service.getSession2().isKilled()) {
                LOGGER.info("{} sql[{}] is killed.", service.toString(), sql);
                service.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
                return;
            }
            this.service.queryCount();
            this.service.getSession2().rowCountRolling();

            if (this.service.getSession2().getRemainingSql() != null) {
                sql = this.service.getSession2().getRemainingSql();
            }
            //Preliminary judgment of multi statement
            if (this.service.isMultiStatementAllow() && this.service.getSession2().generalNextStatement(sql)) {
                sql = sql.substring(0, ParseUtil.findNextBreak(sql));
            }
            String finalSql = sql;
            StatisticListener.getInstance().record(service.getSession2(), r -> r.onFrontendSetSql(service.getSchema(), finalSql));
            this.service.setExecuteSql(sql);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} query sql: {}", service.toString3(), (sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql));
            }
            ShardingServerParse serverParse = ServerParseFactory.getShardingParser();
            int rs = serverParse.parse(sql);
            boolean isWithHint = serverParse.startWithHint(sql);
            int sqlType = rs & 0xff;
            if (isWithHint) {
                service.controlTx(TransactionOperate.QUERY);
                if (sqlType == ServerParse.INSERT || sqlType == ServerParse.DELETE || sqlType == ServerParse.UPDATE ||
                        sqlType == ServerParse.DDL) {
                    if (readOnly) {
                        service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                    }
                }
                service.execute(sql, rs & 0xff);
            } else {
                if (!ShardingServerParse.isTCL(sqlType) &&
                        !ShardingServerParse.isImplicitlyCommitSql(sqlType) &&
                        sqlType != ShardingServerParse.SET) {
                    service.controlTx(TransactionOperate.QUERY);
                }
                switch (sqlType) {
                    //explain sql
                    case ServerParse.EXPLAIN:
                        ExplainHandler.handle(sql, service, rs >>> 8);
                        break;
                    //explain2 shardingnode=? sql=?
                    case ServerParse.EXPLAIN2:
                        Explain2Handler.handle(sql, service, rs >>> 8);
                        break;
                    case ServerParse.DESCRIBE:
                        DescribeHandler.handle(sql, service);
                        break;
                    case ServerParse.SET:
                        SetHandler.handle(sql, service, rs >>> 8);
                        break;
                    case ServerParse.SHOW:
                        ShowHandler.handle(sql, service, rs >>> 8);
                        break;
                    case ServerParse.SELECT:
                        SelectHandler.handle(sql, service, rs >>> 8);
                        break;
                    case ServerParse.START_TRANSACTION:
                    case ServerParse.BEGIN:
                        BeginHandler.handle(sql, service);
                        break;
                    case ServerParse.SAVEPOINT:
                        SavepointHandler.save(sql, service);
                        break;
                    case ServerParse.ROLLBACK_SAVEPOINT:
                        SavepointHandler.rollback(sql, service);
                        break;
                    case ServerParse.RELEASE_SAVEPOINT:
                        SavepointHandler.release(sql, service);
                        break;
                    case ServerParse.KILL:
                        KillHandler.handle(KillHandler.Type.KILL_CONNECTION, sql.substring(rs >>> 8).trim(), service);
                        break;
                    case ServerParse.KILL_QUERY:
                        KillHandler.handle(KillHandler.Type.KILL_QUERY, sql.substring(rs >>> 8).trim(), service);
                        break;
                    case ServerParse.USE:
                        UseHandler.handle(sql, service, rs >>> 8);
                        break;
                    case ServerParse.COMMIT:
                        CommitHandler.handle(sql, service);
                        break;
                    case ServerParse.ROLLBACK:
                        RollBackHandler.handle(sql, service);
                        break;
                    case ServerParse.SCRIPT_PREPARE:
                        ScriptPrepareHandler.handle(sql, service);
                        break;
                    case ServerParse.HELP:
                        LOGGER.info("Unsupported command:" + sql);
                        service.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
                        break;
                    case ServerParse.MYSQL_CMD_COMMENT:
                    case ServerParse.MYSQL_COMMENT:
                        service.writeOkPacket();
                        break;
                    case ServerParse.LOAD_DATA_INFILE_SQL:
                        FrontendConnection connection = (FrontendConnection) service.getConnection();
                        connection.setSkipCheck(true);
                        service.loadDataInfileStart(sql);
                        break;
                    case ServerParse.UNLOCK:
                        service.unLockTable(sql);
                        break;
                    case ServerParse.FLUSH:
                        FlushTableHandler.handle(sql, service);
                        break;
                    case ServerParse.UNSUPPORT:
                        LOGGER.info("Unsupported statement:" + sql);
                        service.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported statement");
                        break;
                    default:
                        if (readOnly) {
                            service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                            break;
                        }
                        service.execute(sql, rs & 0xff);
                }
            }
        } catch (NeedDelayedException e) {
            throw e;
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public ServerQueryHandler(AbstractService service) {
        this.service = (ShardingService) service;
    }

}
