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
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage.XARollbackFailStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage.XARollbackStage;
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
            if (implicitCommitHandler != null) {
                implicitCommitHandler.next();
                return;
            }
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(multiStatementFlag);
            return;
        }

        if (currentStage == null) {
            initXALogEntry();
            changeStageTo(new XAEndStage(session, this, false));
        } else {
            // only for background retry
            changeStageTo(currentStage);
        }

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

        if (currentStage == null) {
            initXALogEntry();
            changeStageTo(new XAEndStage(session, this, true));
            return;
        }

        // only for background retry
        if (currentStage instanceof XARollbackFailStage) {
            changeStageTo(currentStage);
            return;
        }

        if (currentStage instanceof XAEndStage) {
            changeStageTo(new XARollbackStage(session, this, true));
            return;
        }
        interruptTx = false;
        changeStageTo(next());
    }

    @Override
    public void turnOnAutoCommit(byte[] previousSendData) {
        this.packetIfSuccess = previousSendData;
        this.packetId = previousSendData[3];
        this.packetId--;
        this.interruptTx = false;
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
