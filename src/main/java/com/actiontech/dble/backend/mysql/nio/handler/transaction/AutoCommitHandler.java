package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.List;

public class AutoCommitHandler implements TransactionHandler {

    private final NonBlockingSession session;
    private TransactionHandler realHandler;
    private final byte[] sendData;
    private final RouteResultsetNode[] nodes;
    private final List<BackendConnection> errConnection;

    public AutoCommitHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes, List<BackendConnection> errConnection) {
        this.session = session;
        this.sendData = packet;
        this.nodes = nodes;
        this.errConnection = errConnection;
        this.realHandler = session.getTransactionManager().getTransactionHandler();
        this.realHandler.turnOnAutoCommit(packet);
    }

    @Override
    public void commit() {
        realHandler.commit();
    }

    @Override
    public void implicitCommit(ImplicitCommitHandler implicitCommitHandler) {
    }

    @Override
    public void rollback() {
        if (errConnection != null && nodes.length == errConnection.size()) {
            for (BackendConnection conn : errConnection) {
                conn.close(" rollback all connection error");
            }
            session.getTargetMap().clear();
            errConnection.clear();
            session.getSource().write(sendData);
            return;
        }
        if (errConnection != null && errConnection.size() > 0) {
            for (RouteResultsetNode node : nodes) {
                final BackendConnection conn = session.getTarget(node);
                if (errConnection.contains(conn)) {
                    session.getTargetMap().remove(node);
                    conn.close("rollback error connection closed");
                }
            }
            errConnection.clear();
        }
        realHandler.rollback();
    }

    @Override
    public void turnOnAutoCommit(byte[] previousSendData) {
        // no need
    }
}
