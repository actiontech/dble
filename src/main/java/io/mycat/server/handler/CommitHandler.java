package io.mycat.server.handler;

import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.server.ServerConnection;

public final class CommitHandler {
	public static void handle(String stmt, ServerConnection c) {
		c.commit();
		TxnLogHelper.putTxnLog(c, stmt);
		c.setTxstart(false);
		c.getAndIncrementXid();
	}
}
