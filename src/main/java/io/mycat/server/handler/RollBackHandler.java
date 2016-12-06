package io.mycat.server.handler;

import io.mycat.log.transaction.TxnLogHelper;
import io.mycat.server.ServerConnection;

public final class RollBackHandler {
	public static void handle(String stmt, ServerConnection c) {
		c.setTxstart(false);
		TxnLogHelper.putTxnLog(c, stmt);
		c.rollback();
	}
}
