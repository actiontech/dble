package io.mycat.backend.mysql.nio.handler.transaction.xa;

import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public class XAAutoCommitNodesHandler extends XACommitNodesHandler {
	private RouteResultsetNode[] nodes;
//	private Set<BackendConnection> errConnection;
	public XAAutoCommitNodesHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes){
		super(session);
		this.sendData = packet;
		this.nodes = nodes;
	}

	@Override
	protected void nextParse() {
		if (this.isFail()) {
			XAAutoRollbackNodesHandler autoHandler = new XAAutoRollbackNodesHandler(session, sendData, nodes, null);
			autoHandler.rollback();
		} else {
			commit();
		}
	}
}
