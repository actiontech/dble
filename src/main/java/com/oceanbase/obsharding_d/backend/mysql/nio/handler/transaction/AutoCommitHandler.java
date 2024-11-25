/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction;

import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class AutoCommitHandler implements TransactionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoCommitHandler.class);
    private final NonBlockingSession session;
    private TransactionHandler realHandler;
    private final MySQLPacket sendData;
    private final RouteResultsetNode[] nodes;
    private final List<MySQLResponseService> errConnection;

    public AutoCommitHandler(NonBlockingSession session, MySQLPacket packet, RouteResultsetNode[] nodes, List<MySQLResponseService> errConnection) {
        this.session = session;
        this.sendData = packet;
        this.nodes = nodes;
        this.errConnection = errConnection;
        this.realHandler = session.getTransactionManager().getTransactionHandler();
        this.realHandler.turnOnAutoCommit(packet);
    }

    @Override
    public void commit() {
        commit(null);
    }

    @Override
    public void commit(TransactionCallback transactionCallback) {
        // autocommit does not require transactionCallback.callback, so here the parameter is set to null
        realHandler.commit(null);
    }

    @Override
    public void syncImplicitCommit() throws SQLException {
        // ignore
    }

    @Override
    public void rollback() {
        rollback(null);
    }

    @Override
    public void rollback(TransactionCallback transactionCallback) {
        if (errConnection != null && nodes.length == errConnection.size()) {
            for (MySQLResponseService service : errConnection) {
                service.getConnection().businessClose(" rollback all connection error");
            }
            session.getTargetMap().clear();
            errConnection.clear();
            sendData.write(session.getSource());
            return;
        }
        if (errConnection != null && errConnection.size() > 0) {
            for (RouteResultsetNode node : nodes) {
                final BackendConnection conn = session.getTarget(node);
                if (null != conn && errConnection.contains(conn.getBackendService())) {
                    session.getTargetMap().remove(node);
                    conn.businessClose("rollback error connection closed");
                }
            }
            errConnection.clear();
        }
        realHandler.rollback(transactionCallback);
    }

    @Override
    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        // no need
    }
}
