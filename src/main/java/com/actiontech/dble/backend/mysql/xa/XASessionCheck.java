/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.server.NonBlockingSession;

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
