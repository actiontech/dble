package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.ServerParse;
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
            int rs = ServerParse.parse(sql);
            int hintLength = RouteService.isHintSql(sql);
            int sqlType = rs & 0xff;
            if (hintLength >= 0) {
                session.executeHint(sqlType, sql, null);
            } else {
                switch (sqlType) {
                    case ServerParse.USE:
                        String schema = UseHandler.getSchemaName(sql, rs >>> 8);
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setSchema(schema));
                        break;
                    case ServerParse.SHOW:
                    case ServerParse.SELECT:
                        session.execute(false, null);
                        break;
                    case ServerParse.SET:
                        SetHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case ServerParse.LOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setLocked(true));
                        break;
                    case ServerParse.UNLOCK:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setLocked(false));
                        break;
                    case ServerParse.START:
                    case ServerParse.BEGIN:
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setTxStart(true));
                        break;
                    case ServerParse.COMMIT:
                    case ServerParse.ROLLBACK:
                        session.execute(true, (isSuccess, rwSplitService) -> {
                            rwSplitService.getSession().unbindIfSafe(true);
                        });
                        break;
                    case ServerParse.LOAD_DATA_INFILE_SQL:
                        session.getService().setInLoadData(true);
                        session.execute(true, (isSuccess, rwSplitService) -> rwSplitService.setInLoadData(false));
                        break;
                    default:
                        // 1. DDL
                        // 2. DML
                        // 3. procedure
                        // 4. function
                        session.execute(true, null);
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

}
