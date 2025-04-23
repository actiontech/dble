package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.mysql.CommandPacket;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.SetHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.singleton.AppendTraceId;
import com.actiontech.dble.singleton.RouteService;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

import static com.actiontech.dble.net.mysql.MySQLPacket.COM_QUERY;


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
            if (RwSplitServerParse.isMultiStatement(sql)) {
                if ((session.getService().getClientCapabilities() & Capabilities.CLIENT_MULTI_STATEMENTS) == 0) {
                    LOGGER.warn("use multi-query without set CLIENT_MULTI_STATEMENTS flag");
                    session.getService().writeErrMessage(ErrorCode.ERR_WRONG_USED, "Your client must enable multi-query param . For example in jdbc,you should set allowMultiQueries=true in URL.");
                    return;
                }
                session.getService().singleTransactionsCount();
                session.execute(true, null);
                return;
            }
            int rs = RwSplitServerParse.parse(sql);
            int sqlType = rs & 0xff;
            if (AppendTraceId.getInstance().isEnable()) {
                sql = String.format("/*+ trace_id=%d-%d */ %s", session.getService().getConnection().getId(), session.getService().getSqlUniqueId().incrementAndGet(), sql);
            }

            session.getService().setExecuteSql(sql);
            session.endParse();
            int hintLength = RouteService.isHintSql(sql);

            if (hintLength >= 0) {
                session.executeHint(sqlType, sql, null);
            } else {

                if (AppendTraceId.getInstance().isEnable()) {
                    CommandPacket packet = new CommandPacket();
                    packet.setCommand(COM_QUERY);
                    packet.setArg(sql.getBytes());
                    packet.setPacketId(session.getService().getExecuteSqlBytes()[3]);
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    packet.write(out);
                    session.getService().setExecuteSqlBytes(out.toByteArray());
                }
                if (sqlType != RwSplitServerParse.START && sqlType != RwSplitServerParse.BEGIN &&
                        sqlType != RwSplitServerParse.COMMIT && sqlType != RwSplitServerParse.ROLLBACK && sqlType != RwSplitServerParse.SET) {
                    session.getService().singleTransactionsCount();
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
                        int rs2 = RwSplitServerParse.parseSpecial(sqlType, sql);
                        if (rs2 == RwSplitServerParse.SELECT_FOR_UPDATE || rs2 == RwSplitServerParse.LOCK_IN_SHARE_MODE) {
                            session.execute(true, null, false, true);
                        } else {
                            session.execute(null, null, false, true);
                        }
                        break;
                    case RwSplitServerParse.SET:
                        SetHandler.handle(sql, session.getService(), rs >>> 8);
                        break;
                    case RwSplitServerParse.LOCK:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> {
                            if (rwSplitService.isTxStart()) {
                                rwSplitService.setTxStart(false);
                                session.getService().singleTransactionsCount();
                            }
                            rwSplitService.setLocked(true);
                        });
                        break;
                    case RwSplitServerParse.UNLOCK:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setLocked(false));
                        break;
                    case RwSplitServerParse.START_TRANSACTION:
                    case RwSplitServerParse.BEGIN:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setTxStart(true));
                        break;
                    case RwSplitServerParse.COMMIT:
                    case RwSplitServerParse.ROLLBACK:
                        session.execute(true, (isSuccess, resp, rwSplitService) -> {
                            rwSplitService.setTxStart(false);
                            session.getService().singleTransactionsCount();
                        });
                        break;
                    case RwSplitServerParse.LOAD_DATA_INFILE_SQL:
                        session.getService().setInLoadData(true);
                        session.execute(true, (isSuccess, resp, rwSplitService) -> rwSplitService.setInLoadData(false));
                        break;
                    case RwSplitServerParse.HELP:
                        session.execute(null, null);
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
                return (isSuccess, resp, rwSplitService) -> {
                    rwSplitService.setTxStart(false);
                    session.getService().singleTransactionsCount();
                };
            default:
                return null;
        }

    }

}
