package io.mycat.backend.mysql.nio.handler.transaction;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.MultiNodeHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public abstract class AbstractCommitNodesHandler  extends MultiNodeHandler implements CommitNodesHandler {
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommitNodesHandler.class);
	protected ResponseHandler responsehandler;
	public AbstractCommitNodesHandler(NonBlockingSession session) {
		super(session);
		resetResponseHandler();
	}

	@Override
	public void setResponseHandler(ResponseHandler responsehandler) {
		this.responsehandler = responsehandler;
	} 
	protected abstract void preparePhase(MySQLConnection mysqlCon);
	protected abstract void commitPhase(MySQLConnection mysqlCon);
	
	@Override
	public void commit() {
		final int initCount = session.getTargetCount(); 
		lock.lock();
		try {
			reset(initCount);
		} finally {
			lock.unlock();
		} 
		int started = 0;
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				this.setFail("null is contained in RoutResultsetNodes, source = " + session.getSource());
				LOGGER.error(error);
				continue; 
			}
			final BackendConnection conn = session.getTarget(rrn);
			if (conn != null) {
				boolean isClosed = conn.isClosedOrQuit();
				if (isClosed) {
//					session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR,
//							"receive commit,but find backend con is closed or quit"); 
					LOGGER.error(conn + "receive commit,but fond backend con is closed or quit");
					LOGGER.error(error);
					continue;
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("commit job run for " + conn);
				}
				if (clearIfSessionClosed(session)) {
					return;
				}
				conn.setResponseHandler(responsehandler);
				//process the XA_END XA_PREPARE Command
				MySQLConnection mysqlCon = (MySQLConnection) conn;
				if(session.getXaState()==null){
					commitPhase(mysqlCon);
				}
				else{
					switch(session.getXaState()){
					case TX_STARTED_STATE:
						preparePhase(mysqlCon);
						break;
					case TX_PREPARED_STATE:
						commitPhase(mysqlCon);
						break;
					default:
					}
				}
				++started;
			}
		}

		if ((started < nodeCount) && decrementCountBy(initCount - started)) {
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			
			cleanAndFeedback(error.getBytes());
		}
	}
	
	protected void cleanAndFeedback(byte[] ok) {
		// clear all resources
		session.clearResources(false);
		if(session.closed()){
			return;
		}
		if (this.isFail()){
			createErrPkg(error).write(session.getSource());
		} else {
			session.getSource().write(ok);
		}
	}
	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.error("unexpected invocation: connectionAcquired from commit");
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void writeQueueAvailable() {
	
	}
}
