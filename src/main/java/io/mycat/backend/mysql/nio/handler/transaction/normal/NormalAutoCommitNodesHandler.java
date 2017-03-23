package io.mycat.backend.mysql.nio.handler.transaction.normal;

import io.mycat.server.NonBlockingSession;

public class NormalAutoCommitNodesHandler extends NormalCommitNodesHandler {
	public NormalAutoCommitNodesHandler(NonBlockingSession session, byte[] packet) {
		super(session);
		this.sendData = packet;
	}
}
