package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.List;

public class AutoCommitHandler implements TransactionHandler {

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
        realHandler.commit();
    }

    @Override
    public void implicitCommit(ImplicitCommitHandler implicitCommitHandler) {
    }

    @Override
    public void rollback() {
        if (errConnection != null && nodes.length == errConnection.size()) {
            for (MySQLResponseService service : errConnection) {
                service.getConnection().close(" rollback all connection error");
            }
            session.getTargetMap().clear();
            errConnection.clear();
            sendData.write(session.getFrontConnection());
            return;
        }
        if (errConnection != null && errConnection.size() > 0) {
            for (RouteResultsetNode node : nodes) {
                final BackendConnection conn = session.getTarget(node);
                if (errConnection.contains(conn.getBackendService())) {
                    session.getTargetMap().remove(node);
                    conn.close("rollback error connection closed");
                }
            }
            errConnection.clear();
        }
        realHandler.rollback();
    }

    @Override
    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        // no need
    }
}
