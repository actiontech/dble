package io.mycat.server.handler;

import io.mycat.server.ServerConnection;

public final class CommitHandler {
	public static void handle(ServerConnection c) {
    	c.commit();
    	c.setTxstart(false);
    }
}
