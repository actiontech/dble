package io.mycat.backend.mysql.xa;

import io.mycat.server.NonBlockingSession;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class XASessionCheck {
    private final ConcurrentMap<Long, NonBlockingSession> commitSession;
    private final ConcurrentMap<Long, NonBlockingSession> rollbackSession;

    public XASessionCheck() {
        commitSession = new ConcurrentHashMap<>();
        rollbackSession = new ConcurrentHashMap<>();
    }

    public void addCommitSession(NonBlockingSession s) {
        commitSession.put(s.getSource().getId(), s);
    }

    public void addRollbackSession(NonBlockingSession s) {
        this.rollbackSession.put(s.getSource().getId(), s);
    }

    /**
     * 定时执行
     */
    public void checkSessions() {
        checkCommitSession();
        checkRollbackSession();
    }

    private void checkCommitSession() {
        for (NonBlockingSession session : commitSession.values()) {
            if (session.getXaState() == TxState.TX_COMMIT_FAILED_STATE) {
                this.commitSession.remove(session.getSource().getId());
                session.commit();
            }
        }
    }

    private void checkRollbackSession() {
        for (NonBlockingSession session : rollbackSession.values()) {
            this.rollbackSession.remove(session.getSource().getId());
            session.rollback();
        }
    }
}
