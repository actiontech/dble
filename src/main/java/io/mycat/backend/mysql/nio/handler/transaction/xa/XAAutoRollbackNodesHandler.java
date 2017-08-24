package io.mycat.backend.mysql.nio.handler.transaction.xa;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

import java.util.List;

public class XAAutoRollbackNodesHandler extends XARollbackNodesHandler {
    private RouteResultsetNode[] nodes;
    private List<BackendConnection> errConnection;

    public XAAutoRollbackNodesHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes, List<BackendConnection> errConnection) {
        super(session);
        this.sendData = packet;
        this.nodes = nodes;
        this.errConnection = errConnection;
    }

    @Override
    public void rollback() {
        if (errConnection != null && nodes.length == errConnection.size()) {
            for (BackendConnection conn : errConnection) {
                conn.quit();
            }
            errConnection.clear();
            session.getSource().write(sendData);
            return;
        }
        if (errConnection != null && errConnection.size() > 0) {
            for (RouteResultsetNode node : nodes) {
                final BackendConnection conn = session.getTarget(node);
                if (errConnection.contains(conn)) {
                    session.getTargetMap().remove(node);
                    conn.quit();
                }
            }
            errConnection.clear();
        }
        super.rollback();
    }
}
