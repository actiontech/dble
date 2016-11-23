package io.mycat.server.handler;

import io.mycat.server.ServerConnection;

public final class RollBackHandler {
	public static void handle( ServerConnection c) {
		c.rollback();
		c.setTxstart(false);
	}
}
