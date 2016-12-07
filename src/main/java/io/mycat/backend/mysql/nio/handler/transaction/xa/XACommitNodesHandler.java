package io.mycat.backend.mysql.nio.handler.transaction.xa;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;

public class XACommitNodesHandler extends AbstractCommitNodesHandler {

	public XACommitNodesHandler(NonBlockingSession session) {
		super(session);
	}

	@Override
	public void resetResponseHandler() {
		responsehandler = XACommitNodesHandler.this;
	}
	
	@Override
	protected void preparePhase(MySQLConnection mysqlCon) {
		if (mysqlCon.getXaStatus() == TxState.TX_STARTED_STATE) {
			String xaTxId = session.getXaTXID();
			String[] cmds = new String[] { "XA END " + xaTxId, "XA PREPARE " + xaTxId };
			mysqlCon.setXaStatus(TxState.TX_PREPARED_STATE);
			mysqlCon.execBatchCmd(cmds);
		}
	}

	@Override
	protected void commitPhase(MySQLConnection mysqlCon) { 
		String xaTxId = session.getXaTXID();
		String cmd = "XA COMMIT " + xaTxId;
		//send commit
		mysqlCon.setXaStatus(TxState.TX_COMMITED_STATE);
		mysqlCon.execCmd(cmd);

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		MySQLConnection mysqlCon = (MySQLConnection) conn;
		switch (mysqlCon.getXaStatus()) {
		case TX_PREPARED_STATE:
			// if there have many SQL execute wait the okResponse,will come to
			// here one by one
			// should be wait all nodes ready ,then send xa commit to all nodes.
			if (mysqlCon.batchCmdFinished() && decrementCountBy(1)) {
				session.setXaState(TxState.TX_PREPARED_STATE);
				realCommit();
			}
			return;
		case TX_COMMITED_STATE:
			// XA reset status now
			mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
			if (decrementCountBy(1)) {
				session.setXaState(TxState.TX_INITIALIZE_STATE);
				cleanAndFeedback(ok);
			}
			break;
		default:
			// LOGGER.error("Wrong XA status flag!");
		}
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPacket = new ErrorPacket();
		errPacket.read(err);
		String errmsg = new String(errPacket.message);
		this.setFail(errmsg);
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' || 'xa prepare' err
			case TX_PREPARED_STATE:
				if (mysqlCon.batchCmdFinished() && decrementCountBy(1)) {
					realCommit();
				}
				return;
			// 'xa commit' err
			case TX_COMMITED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(errPacket.toBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		LOGGER.warn("backend connect", e);
		String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset()));
		this.setFail(errmsg);
		collectError(conn);
		conn.quit();
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' || 'xa prepare' connectionError,conn now quit
			case TX_PREPARED_STATE:
				if (decrementCountBy(1)) {
					realCommit();
				}
				return;
				// 'xa commit' err
			case TX_COMMITED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(errmsg.getBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		this.setFail(reason);
		collectError(conn);
		if (conn instanceof MySQLConnection) {
			MySQLConnection mysqlCon = (MySQLConnection) conn;
			switch (mysqlCon.getXaStatus()) {
			// 'xa end' || 'xa prepare' connectionClose,conn now quit
			case TX_PREPARED_STATE:
				if (decrementCountBy(1)) {
					realCommit();
				}
				return;
			// 'xa commit' err
			case TX_COMMITED_STATE:
				mysqlCon.setXaStatus(TxState.TX_INITIALIZE_STATE);
				if (decrementCountBy(1)) {
					session.setXaState(TxState.TX_INITIALIZE_STATE);
					cleanAndFeedback(reason.getBytes());
				}
				break;
			default:
				// LOGGER.error("Wrong XA status flag!");
			}
		}
	}

	protected void realCommit(){
		session.setXaState(TxState.TX_PREPARED_STATE);
		if (this.isFail()){
			session.getSource().setTxInterrupt(error);
			createErrPkg(error).write(session.getSource());
		} else {
			commit();
		}
	}
	protected void collectError(BackendConnection conn){
	}
}
