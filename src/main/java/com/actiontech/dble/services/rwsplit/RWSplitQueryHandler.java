/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.route.parser.DbleHintParser;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.services.TransactionOperate;
import com.actiontech.dble.services.rwsplit.handle.RwSplitSelectHandler;
import com.actiontech.dble.services.rwsplit.handle.ScriptPrepareHandler;
import com.actiontech.dble.services.rwsplit.handle.TempTableHandler;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RWSplitQueryHandler implements FrontendQueryHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);
    protected final RwSplitServerParse serverParse = ServerParseFactory.getRwSplitParser();
    protected final RWSplitNonBlockingSession session;

    public RWSplitQueryHandler(RWSplitNonBlockingSession session) {
        this.session = session;
    }

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getService(), "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            session.getService().queryCount();
            session.getService().setExecuteSql(sql);
            session.trace(t -> t.setQuery(sql));
            DbleHintParser.HintInfo hintInfo = DbleHintParser.parseRW(sql);
            int rs = serverParse.parse(sql);
            int sqlType = rs & 0xff;
            if (hintInfo != null) {
                session.executeHint(hintInfo, sqlType, sql, null);
                session.getService().controlTx(TransactionOperate.QUERY);
            } else {
                if (!RwSplitServerParse.isTCL(sqlType) &&
                        !RwSplitServerParse.isImplicitlyCommitSql(sqlType) &&
                        sqlType != RwSplitServerParse.SET) {
                    session.getService().controlTx(TransactionOperate.QUERY);
                }
                switch (sqlType) {
                    case RwSplitServerParse.USE:
                        String schema = UseHandler.getSchemaName(sql, rs >>> 8);
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setSchema(schema));
                        break;
                    case RwSplitServerParse.SHOW:
                        session.execute(true, null, false, true);
                        break;
                    case RwSplitServerParse.SELECT:
                        RwSplitSelectHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.SET:
                        SetHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.LOCK:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> {
                            CallbackFactory.TX_IMPLICITLYCOMMIT.callback(isSuccess, resp, rwSplitService);
                            rwSplitService.setLockTable(true);
                        });
                        break;
                    case RwSplitServerParse.UNLOCK:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setLockTable(false));
                        break;
                    case RwSplitServerParse.START_TRANSACTION:
                    case RwSplitServerParse.BEGIN:
                        session.execute(!session.getService().isReadOnly(), CallbackFactory.TX_START);
                        break;
                    case RwSplitServerParse.COMMIT:
                    case RwSplitServerParse.ROLLBACK:
                        session.execute(true, CallbackFactory.TX_END);
                        break;
                    case RwSplitServerParse.LOAD_DATA_INFILE_SQL:
                        FrontendConnection connection = (FrontendConnection) session.getService().getConnection();
                        connection.setSkipCheck(true);
                        session.getService().setInLoadData(true);
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setInLoadData(false));
                        break;
                    case RwSplitServerParse.HELP:
                        session.execute(null, null);
                        break;
                    case RwSplitServerParse.SCRIPT_PREPARE:
                        ScriptPrepareHandler.handle(session.getService(), sql);
                        break;
                    case RwSplitServerParse.CREATE_TEMPORARY_TABLE:
                        TempTableHandler.handleCreate(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.DROP_TABLE:
                        TempTableHandler.handleDrop(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.XA_START:
                        session.execute(true, null);
                        break;
                    case RwSplitServerParse.XA_COMMIT:
                    case RwSplitServerParse.XA_ROLLBACK:
                        session.execute(true, CallbackFactory.XA_END);
                        break;
                    default:
                        // 1. DDL
                        // 2. DML
                        // 3. procedure
                        // 4. function
                        session.execute(true, handleCallback(sqlType));
                        break;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("execute error", e);
            session.getService().writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private Callback handleCallback(int sqlType) {
        if (RwSplitServerParse.isImplicitlyCommitSql(sqlType)) {
            return CallbackFactory.TX_IMPLICITLYCOMMIT;
        }
        return null;
    }
}
