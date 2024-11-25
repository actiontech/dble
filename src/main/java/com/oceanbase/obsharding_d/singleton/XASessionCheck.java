/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage.XAStage;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class XASessionCheck {
    private static final XASessionCheck INSTANCE = new XASessionCheck();

    private final ConcurrentMap<Long, NonBlockingSession> commitSession;
    private final ConcurrentMap<Long, NonBlockingSession> rollbackSession;
    private final ConcurrentMap<Long, NonBlockingSession> committingSession;
    private final ConcurrentMap<Long, NonBlockingSession> rollbackingSession;

    private XASessionCheck() {
        commitSession = new ConcurrentHashMap<>();
        rollbackSession = new ConcurrentHashMap<>();
        committingSession = new ConcurrentHashMap<>();
        rollbackingSession = new ConcurrentHashMap<>();
    }

    public void addCommitSession(NonBlockingSession s) {
        this.commitSession.put(s.getSource().getId(), s);
        this.committingSession.put(s.getSource().getId(), s);
    }

    public void addRollbackSession(NonBlockingSession s) {
        this.rollbackSession.put(s.getSource().getId(), s);
        this.rollbackingSession.put(s.getSource().getId(), s);
    }

    public void checkSessions() {
        checkCommitSession();
        checkRollbackSession();
    }

    private void checkCommitSession() {
        String xaStage;
        for (NonBlockingSession session : commitSession.values()) {
            xaStage = session.getTransactionManager().getXAStage();
            if (XAStage.COMMIT_FAIL_STAGE.equals(xaStage)) {
                this.commitSession.remove(session.getSource().getId());
                session.commit(null);
            }
        }
    }

    private void checkRollbackSession() {
        for (NonBlockingSession session : rollbackSession.values()) {
            this.rollbackSession.remove(session.getSource().getId());
            session.rollback(null);
        }
    }

    public ConcurrentMap<Long, NonBlockingSession> getCommitSession() {
        return commitSession;
    }

    public ConcurrentMap<Long, NonBlockingSession> getRollbackSession() {
        return rollbackSession;
    }

    public ConcurrentMap<Long, NonBlockingSession> getCommittingSession() {
        return committingSession;
    }

    public ConcurrentMap<Long, NonBlockingSession> getRollbackingSession() {
        return rollbackingSession;
    }

    public static XASessionCheck getInstance() {
        return INSTANCE;
    }

}
