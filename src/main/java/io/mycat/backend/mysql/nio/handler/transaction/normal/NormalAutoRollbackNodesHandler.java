package io.mycat.backend.mysql.nio.handler.transaction.normal;

import java.util.Set;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public class NormalAutoRollbackNodesHandler extends NormalRollbackNodesHandler{
	private RouteResultsetNode[] nodes;
	private Set<BackendConnection> errConnection;
	public NormalAutoRollbackNodesHandler(NonBlockingSession session, byte[] packet ,RouteResultsetNode[] nodes,Set<BackendConnection> errConnection) {
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
			for (int i = 0; i < nodes.length; i++) {
				RouteResultsetNode node = nodes[i];
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
