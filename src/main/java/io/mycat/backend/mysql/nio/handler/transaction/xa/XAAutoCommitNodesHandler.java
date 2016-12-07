package io.mycat.backend.mysql.nio.handler.transaction.xa;

import java.util.HashSet;
import java.util.Set;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

public class XAAutoCommitNodesHandler extends XACommitNodesHandler {
	private RouteResultsetNode[] nodes;
	private byte[] sendData;
	private Set<BackendConnection> errConnection;
	public XAAutoCommitNodesHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes){
		super(session);
		this.sendData = packet;
		this.nodes = nodes;
	}
	@Override
	public void resetResponseHandler() {
		responsehandler = XAAutoCommitNodesHandler.this;
	}
	@Override
	protected void cleanAndFeedback(byte[] ok) {
		// clear all resources
		session.clearResources(false);
		if (session.closed()) {
			return;
		}
		if (this.isFail()) {
			createErrPkg(error).write(session.getSource());
		} else {
			session.getSource().write(sendData);
		}
	}
	@Override
	protected void realCommit(){
		session.setXaState(TxState.TX_PREPARED_STATE);
		if (this.isFail()){ 
			XAAutoRollbackNodesHandler  autoHandler = new XAAutoRollbackNodesHandler(session, sendData, nodes, errConnection);
			autoHandler.rollback();
		} else {
			commit();
		}
	}

	protected void collectError(BackendConnection conn){
		lock.lock();
		try {
			if(errConnection==null){
				errConnection = new HashSet<BackendConnection>();
			}
			errConnection.add(conn);
		} finally {
			lock.unlock();
		} 
		
	}
}
