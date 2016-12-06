package io.mycat.backend.mysql.nio.handler.transaction;

import io.mycat.backend.mysql.nio.handler.ResponseHandler;

public interface RollbackNodesHandler{

	void setResponseHandler(ResponseHandler responsehandler);

	void rollback();

	void resetResponseHandler();
}