package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RWSplitQueryHandler implements FrontendQueryHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final RWSplitNonBlockingSession session;
    //private Boolean readOnly = true;
    //private boolean sessionReadOnly = true;

    @Override
    public void setReadOnly(Boolean readOnly) {
        //this.readOnly = readOnly;
    }

    @Override
    public void setSessionReadOnly(boolean sessionReadOnly) {
        // this.sessionReadOnly = sessionReadOnly;
    }

    public RWSplitQueryHandler(RWSplitNonBlockingSession session) {
        this.session = session;
    }

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getService(), "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            int rs = ServerParse.parse(sql);
            int sqlType = rs & 0xff;
            switch (sqlType) {
                case ServerParse.USE:
                    String schema = UseHandler.getSchemaName(sql, rs >>> 8);
                    session.execute(false, rwSplitService -> rwSplitService.setSchema(schema));
                    break;
                case ServerParse.SHOW:
                    session.execute(true, null);
                    break;
                case ServerParse.SELECT:
                    session.execute(parseSelectQuery(sql), null);
                    break;
                case ServerParse.SET:
                    parseSet(sql);
                    break;
                case ServerParse.LOCK:
                    session.execute(false, rwSplitService -> rwSplitService.setLocked(true));
                    break;
                case ServerParse.UNLOCK:
                    session.execute(false, rwSplitService -> rwSplitService.setLocked(false));
                    break;
                case ServerParse.COMMIT:
                case ServerParse.ROLLBACK:
                    session.execute(false, rwSplitService -> rwSplitService.getSession().unbindIfSafe());
                    break;
                default:
                    // 1. DDL
                    session.execute(false, null);
                    break;
            }
        } catch (Exception e) {
            LOGGER.warn("execute error", e);
            session.getService().writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, e.getMessage());
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    private boolean parseSelectQuery(String sql) {
        boolean canSelectSlave = false;
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement(true);
        if (statement instanceof SQLSelectStatement) {
            if (!((SQLSelectStatement) statement).getSelect().getQueryBlock().isForUpdate()) {
                canSelectSlave = true;
            }
        } else {
            LOGGER.warn("unknown select");
            throw new UnsupportedOperationException("unknown");
        }

        return canSelectSlave;
    }

    private void parseSet(String sql) throws IOException {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement(true);
        if (statement instanceof SQLSetStatement) {
            List<SQLAssignItem> assignItems = ((SQLSetStatement) statement).getItems();
            if (assignItems.size() == 1) {
                SQLAssignItem item = assignItems.get(0);
                if (item.getTarget().toString().equalsIgnoreCase("autocommit")) {
                    if (session.getService().isAutocommit() && item.getValue().toString().equalsIgnoreCase("0")) {
                        session.getService().setAutocommit(false);
                        session.getService().writeDirectly(OkPacket.OK);
                    }
                    if (!session.getService().isAutocommit() && item.getValue().toString().equalsIgnoreCase("1")) {
                        session.execute(false, rwSplitService -> rwSplitService.setAutocommit(true));
                    }
                }
            } else {
                throw new UnsupportedOperationException("unknown");
            }
        } else {
            throw new UnsupportedOperationException("unknown");
        }
    }

}
