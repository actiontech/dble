package io.mycat.backend.mysql.nio.handler.transaction.normal;

import io.mycat.server.NonBlockingSession;

public class NormalAutoCommitNodesHandler extends NormalCommitNodesHandler {
	private byte[] sendData;
	public NormalAutoCommitNodesHandler(NonBlockingSession session, byte[] packet) {
		super(session);
		this.sendData = packet;
	}
	@Override
	public void resetResponseHandler() {
		responsehandler = NormalAutoCommitNodesHandler.this;
	}

	@Override
	protected void cleanAndFeedback(byte[] ok) {
		// clear all resources
		session.clearResources(false);
		if (session.closed()) {
			return;
		}
		if (this.isFail()) {
			createErrPkg(error).write(session.getSource());
		} else {
			session.getSource().write(sendData);
		}
	}
}
