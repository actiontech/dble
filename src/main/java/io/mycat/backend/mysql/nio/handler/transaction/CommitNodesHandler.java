package io.mycat.backend.mysql.nio.handler.transaction;

import io.mycat.backend.mysql.nio.handler.ResponseHandler;

public interface CommitNodesHandler {
	void setResponseHandler(ResponseHandler responsehandler);

	void commit();

	void resetResponseHandler();
}
