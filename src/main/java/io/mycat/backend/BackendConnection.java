package io.mycat.backend;

import java.io.UnsupportedEncodingException;

import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.net.ClosableConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;

public interface BackendConnection extends ClosableConnection {
	boolean isModifiedSQLExecuted();

	boolean isDDL();

	boolean isFromSlaveDB();

	String getSchema();

	void setSchema(String newSchema);

	long getLastTime();

	boolean isClosedOrQuit();

	void setAttachment(Object attachment);

	void quit();

	void setLastTime(long currentTimeMillis);

	void release();

	boolean setResponseHandler(ResponseHandler commandHandler);

	void commit();

	void query(String sql) throws UnsupportedEncodingException;

	Object getAttachment();

	// long getThreadId();



	void execute(RouteResultsetNode node, ServerConnection source,
			boolean autocommit);

	boolean syncAndExcute();

	void rollback();

	boolean isBorrowed();

	void setBorrowed(boolean borrowed);

	int getTxIsolation();

	boolean isAutocommit();

	long getId();

	void terminate(String reason);

	String compactInfo();
}
