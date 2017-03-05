package io.mycat.server.handler;

import io.mycat.config.ErrorCode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;

public class DescribeHandler {
	public static void handle(String stmt, ServerConnection c) {
		String db = c.getSchema(); 
		if (db == null) { 
			c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
			return;
		}
		c.execute(stmt, ServerParse.DESCRIBE);
	}
}
