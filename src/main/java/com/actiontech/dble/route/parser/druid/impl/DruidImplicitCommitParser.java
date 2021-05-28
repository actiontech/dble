package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.backend.mysql.nio.handler.ExecutableHandler;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeDdlPrepareHandler;
import com.actiontech.dble.backend.mysql.nio.handler.SingleNodeDDLHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.sql.SQLException;
import java.util.Optional;

public class DruidImplicitCommitParser extends DefaultDruidParser {
    private boolean isSyntaxNotSupported = false;  // Syntax not supported or error
    private SQLException sqlException;

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        if (service.getSession2().getSessionXaID() != null) {
            // Implicitly committed statement (such as: DDL) is not allowed to be executed in xa transaction.
            throw new SQLException("The command cannot be executed when xa transaction.");
        }
        try {
            schema = doVisitorParse(schema, rrs, stmt, visitor, service, isExplain);
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            if (sqlException != null) {
                if (!isSyntaxNotSupported &&
                        sqlException.getErrorCode() != 0 &&
                        sqlException.getErrorCode() != ErrorCode.ER_NO_DB_ERROR &&
                        sqlException.getErrorCode() != ErrorCode.ER_PARSE_ERROR) {
                    // Implicit commit does not take effect if a syntax error occurs or if a library is not selected
                    service.getSession2().syncImplicitCommit();
                    service.transactionsCountInTx();
                    resetTxState(service);
                }
                throw sqlException;
            } else {
                service.getSession2().syncImplicitCommit();
                service.transactionsCountInTx();
                resetTxState(service);
                if (rrs.isFinishedExecute()) {
                    service.writeOkPacket();
                } else {
                    rrs.setFinishedRoute(true);
                    rrs.setDdlHandler(visitorParseEnd(rrs, service));
                }
            }
        }
        return schema;
    }

    public SchemaConfig doVisitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        return schema;
    }

    public ExecutableHandler visitorParseEnd(RouteResultset rrs, ShardingService service) {
        if (rrs.getNodes().length == 1) {
            return new SingleNodeDDLHandler(rrs, service.getSession2());
        } else {
            return new MultiNodeDdlPrepareHandler(rrs, service.getSession2());
        }
    }

    public void checkSchema(String schema) throws SQLException {
        if (StringUtil.isEmpty(schema)) {
            isSyntaxNotSupported = true;
            throw new SQLException("No database selected", "3D000", ErrorCode.ER_NO_DB_ERROR);
        }
    }

    public boolean isSyntaxNotSupported() {
        return isSyntaxNotSupported;
    }

    public void setSyntaxNotSupported(boolean syntaxNotSupported) {
        isSyntaxNotSupported = syntaxNotSupported;
    }

    private void resetTxState(ShardingService service) {
        if (service.isTxStart()) {
            service.setTxStart(false);
            Optional.ofNullable(StatisticListener.getInstance().getRecorder(service)).ifPresent(r -> r.onTxEnd());
            service.getAndIncrementXid();
            if (!service.isAutocommit()) {
                Optional.ofNullable(StatisticListener.getInstance().getRecorder(service)).ifPresent(r -> r.onTxStartByImplicitly(service));
            }
        }
    }
}
