package io.mycat.backend.mysql.nio.handler.transaction;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.MultiNodeHandler;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public abstract class AbstractRollbackNodesHandler extends MultiNodeHandler implements RollbackNodesHandler {

	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractRollbackNodesHandler.class);

	public AbstractRollbackNodesHandler(NonBlockingSession session) {
		super(session);
	}
	protected abstract void executeRollback(MySQLConnection mysqlCon, int position);
	public void rollback() {
		final int initCount = session.getTargetCount();
		lock.lock();
		try {
			reset(initCount);
		} finally {
			lock.unlock();
		}
		// 执行
		int position = 0;
		for (final RouteResultsetNode node : session.getTargetKeys()) {
			final BackendConnection conn = session.getTarget(node);
			conn.setResponseHandler(this);
			executeRollback((MySQLConnection) conn, position++);
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
		LOGGER.error("unexpected invocation: connectionAcquired from rollback");
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