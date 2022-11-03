/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
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
    public void commit(ImplicitHandler implicitHandler) {
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
    public void rollback(ImplicitHandler implicitHandler) {
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
        realHandler.rollback(implicitHandler);
    }

    @Override
    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        // no need
    }
}
