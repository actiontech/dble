/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.handler.*;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
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
    private boolean sessionReadOnly = true;

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setSessionReadOnly(boolean sessionReadOnly) {
        this.sessionReadOnly = sessionReadOnly;
    }

    public ServerQueryHandler(AbstractService service) {
        this.service = (ShardingService) service;
    }

    @Override
    public void query(String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service + sql);
        }
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            if (service.getSession2().isKilled()) {
                LOGGER.info("sql[" + sql + "] is killed.");
                service.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
                return;
            }
            this.service.queryCount();
            this.service.getSession2().rowCountRolling();

            if (this.service.getSession2().getRemingSql() != null) {
                sql = this.service.getSession2().getRemingSql();
            }
            //Preliminary judgment of multi statement
            if (this.service.isMultiStatementAllow() && this.service.getSession2().generalNextStatement(sql)) {
                sql = sql.substring(0, ParseUtil.findNextBreak(sql));
            }
            this.service.setExecuteSql(sql);

            int rs = ServerParse.parse(sql);
            boolean isWithHint = ServerParse.startWithHint(sql);
            int sqlType = rs & 0xff;
            if (isWithHint) {
                if (sqlType == ServerParse.INSERT || sqlType == ServerParse.DELETE || sqlType == ServerParse.UPDATE ||
                        sqlType == ServerParse.DDL) {
                    if (readOnly) {
                        service.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User READ ONLY");
                    } else if (sessionReadOnly) {
                        service.writeErrMessage(ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, "Cannot execute statement in a READ ONLY transaction.");
                    }
                }
                service.execute(sql, rs & 0xff);
            } else {
                if (sqlType != ServerParse.START && sqlType != ServerParse.BEGIN &&
                        sqlType != ServerParse.COMMIT && sqlType != ServerParse.ROLLBACK && sqlType != ServerParse.SET) {
                    this.service.singleTransactionsCount();
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
                    case ServerParse.START:
                        StartHandler.handle(sql, service, rs >>> 8);
                        break;
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
                        service.loadDataInfileStart(sql);
                        break;
                    case ServerParse.LOCK:
                        service.lockTable(sql);
                        break;
                    case ServerParse.UNLOCK:
                        service.unLockTable(sql);
                        break;
                    case ServerParse.CREATE_VIEW:
                    case ServerParse.REPLACE_VIEW:
                    case ServerParse.ALTER_VIEW:
                    case ServerParse.DROP_VIEW:
                        ViewHandler.handle(sqlType, sql, service);
                        break;
                    case ServerParse.CREATE_DATABASE:
                        CreateDatabaseHandler.handle(sql, service);
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
                        } else if (sessionReadOnly) {
                            service.writeErrMessage(ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, "Cannot execute statement in a READ ONLY transaction.");
                            break;
                        }
                        service.execute(sql, rs & 0xff);
                }
            }
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

}
