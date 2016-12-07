package io.mycat.backend.mysql.nio.handler.transaction.normal;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;

public class NormalCommitNodesHandler extends AbstractCommitNodesHandler{

	public NormalCommitNodesHandler(NonBlockingSession session) {
		super(session);
	}

	@Override
	public void resetResponseHandler() {
		responsehandler = NormalCommitNodesHandler.this;
	}

	@Override
	protected void preparePhase(MySQLConnection mysqlCon) {
		//need not prepare, do nothing
	}

	@Override
	protected void commitPhase(MySQLConnection mysqlCon) {
		mysqlCon.commit();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (decrementCountBy(1)) { 
			cleanAndFeedback(ok);
		}
	}
	@Override
	public void errorResponse(byte[] err, BackendConnection conn){
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(err);
		String errmsg = new String(errPacket.message);
		this.setFail(errmsg);
		if (decrementCountBy(1)) {
			cleanAndFeedback(errPacket.toBytes());
		}
	}
	@Override
	public void connectionError(Throwable e, BackendConnection conn){
		LOGGER.warn("backend connect", e);
		String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset()));
		this.setFail(errmsg);
		conn.quit();
		if (decrementCountBy(1)) {
			cleanAndFeedback(errmsg.getBytes());
		}
	}
	@Override
	public void connectionClose(BackendConnection conn, String reason){
		this.setFail(reason);
		if (decrementCountBy(1)) {
			cleanAndFeedback(reason.getBytes());
		}
	}
}
