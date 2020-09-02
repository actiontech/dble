package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.UseHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RWSplitQueryHandler implements FrontendQueryHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryHandler.class);

    private final RWSplitService service;
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

    public RWSplitQueryHandler(AbstractService service) {
        this.service = (RWSplitService) service;
    }

    @Override
    public void query(String sql) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "handle-query-sql");
        TraceManager.log(ImmutableMap.of("sql", sql), traceObject);
        try {
            int rs = ServerParse.parse(sql);
            int sqlType = rs & 0xff;
            boolean canSelectSlave = false;
            Callback callback = null;
            switch (sqlType) {
                case ServerParse.USE:
                    String schema = UseHandler.getSchemaName(sql, rs >>> 8);
                    callback = rwSplitService -> service.setSchema(schema);
                    break;
                case ServerParse.SHOW:
                    canSelectSlave = true;
                    break;
                case ServerParse.SELECT:
                    canSelectSlave = parseSelectQuery(sql);
                    break;
                case ServerParse.SET:
                    // todo
                    canSelectSlave = false;
                    break;
                //                case ServerParse.LOCK:
                //                    canSelectSlave = false;
                //                    break;
                //                case ServerParse.UNLOCK:
                //                    canSelectSlave = false;
                //                break;
                default:
                    break;
            }

            service.execute(canSelectSlave, callback);
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
        }

        return canSelectSlave;
    }

}
