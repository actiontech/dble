package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.TraceManager;
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
        try {
            session.getService().queryCount();
            int rs = RwSplitServerParse.parse(sql);
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
                    case RwSplitServerParse.SELECT:
                        session.execute(null, null);
                        break;
                    case RwSplitServerParse.SET:
                        SetHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.LOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            if (rwSplitService.isTxStart()) {
                                rwSplitService.setTxStart(false);
                                session.getService().singleTransactionsCount();
                            }
                            rwSplitService.setLocked(true);
                        });
                        break;
                    case RwSplitServerParse.UNLOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setLocked(false));
                        break;
                    case RwSplitServerParse.START_TRANSACTION:
                    case RwSplitServerParse.BEGIN:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setTxStart(true));
                        break;
                    case RwSplitServerParse.COMMIT:
                    case RwSplitServerParse.ROLLBACK:
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            rwSplitService.setTxStart(false);
                            session.getService().singleTransactionsCount();
                        });
                        break;
                    case RwSplitServerParse.LOAD_DATA_INFILE_SQL:
                        session.getService().setInLoadData(true);
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setInLoadData(false));
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
                    rwSplitService.setTxStart(false);
                    session.getService().singleTransactionsCount();
                };
            default:
                return null;
        }

    }

}
