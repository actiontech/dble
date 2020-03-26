/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage.XAEndStage;
import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;
import com.actiontech.dble.backend.mysql.xa.ParticipantLogEntry;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public class XAHandler extends AbstractXAHandler implements TransactionHandler {

    public XAHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void commit() {
        if (session.getTargetCount() <= 0) {
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(multiStatementFlag);
            return;
        }

        // get session's lock before sending commit(in fact, after ended)
        // then the XA transaction will be not killed, if killed ,then we will not commit
        if (currentStage instanceof XAEndStage) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }
        initXALogEntry();
        changeStageTo(new XAEndStage(session, this, false));
    }

    @Override
    public void implicitCommit(ImplicitCommitHandler handler) {
        this.implicitCommitHandler = handler;
        commit();
    }

    @Override
    public void rollback() {
        if (session.getTargetCount() <= 0) {
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(multiStatementFlag);
            return;
        }

        // get session's lock before sending rollback(in fact, after ended)
        // then the XA transaction will be not killed. if killed ,then we will not rollbacks
        if (currentStage == null) {
            initXALogEntry();
            changeStageTo(new XAEndStage(session, this, true));
        } else {
            if (currentStage instanceof XAEndStage &&
                    !session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
            interruptTx = false;
            ((XAEndStage) currentStage).setRollback(true);
            changeStageTo(next());
        }
    }

    @Override
    public void turnOnAutoCommit(byte[] previousSendData) {
        packetIfSuccess = previousSendData;
        interruptTx = false;
    }

    private void initXALogEntry() {
        CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(),
                new ParticipantLogEntry[session.getTargetCount()], TxState.TX_STARTED_STATE);
        XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);

        int position = 0;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);

            XAStateLog.initRecoveryLog(session.getSessionXaID(), position, (MySQLConnection) conn);
            position++;
        }
    }

}
