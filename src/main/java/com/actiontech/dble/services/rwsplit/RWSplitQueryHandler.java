package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.services.rwsplit.handle.RwSplitSelectHandler;
import com.actiontech.dble.services.rwsplit.handle.TempTableHandler;
import com.actiontech.dble.services.rwsplit.handle.XaHandler;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RWSplitQueryHandler implements FrontendQueryHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final RWSplitNonBlockingSession session;

    public RWSplitQueryHandler(RWSplitNonBlockingSession session) {
        this.session = session;
    }

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getService(), "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        StatisticListener.getInstance().record(session, r -> r.onFrontendSetSql(session.getService().getSchema(), sql));
        try {
            RwSplitServerParse serverParse = ServerParseFactory.getRwSplitParser();
            session.getService().queryCount();
            if (serverParse.isMultiStatement(sql)) {
                if (!session.getService().isMultiStatementAllow()) {
                    LOGGER.warn("use multi-query without set CLIENT_MULTI_STATEMENTS flag");
                    session.getService().writeErrMessage(ErrorCode.ERR_WRONG_USED, "Your client must enable multi-query param .For example in jdbc,you should set allowMultiQueries=true in URL.");
                    return;
                }
                session.getService().singleTransactionsCount();
                StatisticListener.getInstance().record(session.getService(), r -> r.onFrontendMultiSqlStart());
                session.execute(true, null);
                return;
            }
            int rs = serverParse.parse(sql);
            int hintLength = RouteService.isHintSql(sql);
            int sqlType = rs & 0xff;
            if (hintLength >= 0) {
                session.executeHint(sqlType, sql, null);
            } else {
                if (sqlType != RwSplitServerParse.START && sqlType != RwSplitServerParse.BEGIN &&
                        sqlType != RwSplitServerParse.COMMIT && sqlType != RwSplitServerParse.ROLLBACK && sqlType != RwSplitServerParse.SET) {
                    session.getService().singleTransactionsCount();
                }
                switch (sqlType) {
                    case RwSplitServerParse.USE:
                        String schema = UseHandler.getSchemaName(sql, rs >>> 8);
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setSchema(schema));
                        break;
                    case RwSplitServerParse.SHOW:
                        session.execute(true, null, false);
                        break;
                    case RwSplitServerParse.SELECT:
                        RwSplitSelectHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.SET:
                        SetHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.LOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            rwSplitService.implicitlyDeal();
                            rwSplitService.setLocked(true);
                        });
                        break;
                    case RwSplitServerParse.UNLOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setLocked(false));
                        break;
                    case RwSplitServerParse.START_TRANSACTION:
                    case RwSplitServerParse.BEGIN:
                        StatisticListener.getInstance().record(session, r -> r.onTxPreStart());
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            boolean isImplicitly = false;
                            if (rwSplitService.isTxStart() || !rwSplitService.isAutocommit()) {
                                isImplicitly = true;
                                StatisticListener.getInstance().record(session, r -> r.onTxEnd());
                            }
                            rwSplitService.getAndIncrementTxId();
                            rwSplitService.setTxStart(true);
                            if (isImplicitly) {
                                StatisticListener.getInstance().record(session, r -> r.onTxStartByImplicitly(rwSplitService));
                            } else {
                                StatisticListener.getInstance().record(session, r -> r.onTxStart(rwSplitService));
                            }
                        });
                        break;
                    case RwSplitServerParse.COMMIT:
                    case RwSplitServerParse.ROLLBACK:
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            rwSplitService.setTxStart(false);
                            rwSplitService.singleTransactionsCount();
                            StatisticListener.getInstance().record(session, r -> r.onTxEnd());
                            if (!rwSplitService.isAutocommit()) {
                                rwSplitService.getAndIncrementTxId();
                                StatisticListener.getInstance().record(session, r -> r.onTxStartByImplicitly(rwSplitService));
                            }
                        });
                        break;
                    case RwSplitServerParse.LOAD_DATA_INFILE_SQL:
                        FrontendConnection connection = (FrontendConnection) session.getService().getConnection();
                        connection.setSkipCheck(true);
                        session.getService().setInLoadData(true);
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setInLoadData(false));
                        break;
                    case RwSplitServerParse.HELP:
                        session.execute(null, null);
                        break;
                    case RwSplitServerParse.SCRIPT_PREPARE:
                        session.execute(true, null, sql);
                        break;
                    case RwSplitServerParse.CREATE_TEMPORARY_TABLE:
                        TempTableHandler.handleCreate(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.DROP_TABLE:
                        TempTableHandler.handleDrop(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.XA_START:
                        XaHandler.xaStart(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.XA_COMMIT:
                    case RwSplitServerParse.XA_ROLLBACK:
                        XaHandler.xaFinish(session.getService());
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
        switch (sqlType) {
            case RwSplitServerParse.DDL:
            case RwSplitServerParse.ALTER_VIEW:
            case RwSplitServerParse.CREATE_DATABASE:
            case RwSplitServerParse.CREATE_VIEW:
            case RwSplitServerParse.DROP_VIEW:
            case RwSplitServerParse.INSTALL:
            case RwSplitServerParse.RENAME:
            case RwSplitServerParse.UNINSTALL:
            case RwSplitServerParse.GRANT:
            case RwSplitServerParse.REVOKE:
                return (isSuccess, rwSplitService) -> {
                    rwSplitService.implicitlyDeal();
                };
            default:
                return null;
        }

    }

}
