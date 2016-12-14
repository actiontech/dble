package io.mycat.backend.mysql.xa;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.mycat.server.NonBlockingSession;

public class XASessionCheck {
	private final ConcurrentMap<Long, NonBlockingSession> commitSession;
	private final ConcurrentMap<Long, NonBlockingSession> rollbackSession;
	public XASessionCheck(){
		commitSession = new ConcurrentHashMap<Long, NonBlockingSession>();
		rollbackSession = new ConcurrentHashMap<Long, NonBlockingSession>();
	}
	
	public void addCommitSession(NonBlockingSession s) {
		commitSession.put(s.getSource().getId(), s);
	}
	
	public void addRollbackSession(NonBlockingSession s) {
		this.rollbackSession.put(s.getSource().getId(), s);
	}
	
	public void removeCommitSession(NonBlockingSession s) {
		this.commitSession.remove(s.getSource().getId());
	}
	
	public void removeRollbackSession(NonBlockingSession s) {
		this.rollbackSession.remove(s.getSource().getId());
	}
	/**
	 * 定时执行
	 */
	public void checkSessions() {
		checkCommitSession();
		checkRollbackSession();
	}
	private void checkCommitSession(){
		Iterator<NonBlockingSession> itertor = commitSession.values().iterator();
		while (itertor.hasNext()) {
			NonBlockingSession session = itertor.next();
			if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
				this.commitSession.remove(session.getSource().getId());
				session.commit();
			}
		}
	}
	private void checkRollbackSession(){
		Iterator<NonBlockingSession> itertor = rollbackSession.values().iterator();
		while (itertor.hasNext()) {
			NonBlockingSession session = itertor.next();
			this.rollbackSession.remove(session.getSource().getId());
			session.rollback();
		}
	}
}
