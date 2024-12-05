/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.rwsplit;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.OBsharding_DHintParser;
import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;
import com.oceanbase.obsharding_d.rwsplit.RWSplitNonBlockingSession;
import com.oceanbase.obsharding_d.server.parser.RwSplitServerParse;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.sql.StatisticListener;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.ImmutableMap;

public final class RWSplitMultiQueryHandler extends RWSplitQueryHandler {

    public RWSplitMultiQueryHandler(RWSplitNonBlockingSession session) {
        super(session);
    }

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getService(), "handle-multi-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            session.getService().queryCount();
            if (!session.getService().isMultiStatementAllow()) {
                LOGGER.warn("use multi-query without set CLIENT_MULTI_STATEMENTS flag");
                session.getService().writeErrMessage(ErrorCode.ERR_WRONG_USED, "Your client must enable multi-query param .For example in jdbc,you should set allowMultiQueries=true in URL.");
                return;
            }
            if (session.getRemainingSql() != null) {
                sql = session.getRemainingSql();
            }
            if (session.generalNextStatement(sql)) {
                sql = sql.substring(0, ParseUtil.findNextBreak(sql));
            }
            String finalSql = sql.trim();
            session.getService().setExecuteSql(finalSql);
            StatisticListener.getInstance().record(session, r -> r.onFrontendSetSql(session.getService().getSchema(), finalSql));
            OBsharding_DHintParser.HintInfo hintInfo = OBsharding_DHintParser.parseRW(finalSql);
            int rs = serverParse.parse(finalSql);
            int sqlType = rs & 0xff;
            Callback callback = CallbackFactory.DEFAULT;
            if (hintInfo != null) {
                // not deal
            } else {
                switch (sqlType) {
                    case RwSplitServerParse.START_TRANSACTION:
                    case RwSplitServerParse.BEGIN:
                        callback = CallbackFactory.TX_START;
                        break;
                    case RwSplitServerParse.COMMIT:
                    case RwSplitServerParse.ROLLBACK:
                        callback = CallbackFactory.TX_END;
                        break;
                    case RwSplitServerParse.XA_START:
                        callback = (isSuccess, resp, rwSplitService) -> {
                            if (isSuccess) {
                                String xaId = StringUtil.removeAllApostrophe(rwSplitService.getExecuteSql().substring(rs >>> 8).trim());
                                StatisticListener.getInstance().record(rwSplitService, r -> r.onXaStart(xaId));
                            }
                        };
                        break;
                    case RwSplitServerParse.XA_COMMIT:
                    case RwSplitServerParse.XA_ROLLBACK:
                        callback = CallbackFactory.XA_END;
                        break;
                    default:
                        if (RwSplitServerParse.isImplicitlyCommitSql(sqlType)) {
                            callback = CallbackFactory.TX_IMPLICITLYCOMMIT;
                            break;
                        }
                        int res = RwSplitServerParse.isSetAutocommitSql(sql);
                        if (res == 1) {
                            if (!session.getService().isAutocommit()) {
                                callback = CallbackFactory.TX_AUTOCOMMIT;
                                break;
                            }
                        } else if (res == 0) {
                            if (session.getService().isAutocommit()) {
                                callback = CallbackFactory.TX_UN_AUTOCOMMIT;
                                break;
                            }
                        }
                        break;
                }
            }
            session.executeMultiSql(true, callback);
            return;
        } catch (Exception e) {
            LOGGER.warn("execute error", e);
            session.getService().writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }
}
