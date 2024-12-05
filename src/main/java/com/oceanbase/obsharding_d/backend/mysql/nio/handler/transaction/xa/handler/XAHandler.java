/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.handler;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage.CommitStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage.RollbackStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage.XAEndStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage.XARollbackFailStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage.XARollbackStage;
import com.oceanbase.obsharding_d.backend.mysql.xa.CoordinatorLogEntry;
import com.oceanbase.obsharding_d.backend.mysql.xa.ParticipantLogEntry;
import com.oceanbase.obsharding_d.backend.mysql.xa.TxState;
import com.oceanbase.obsharding_d.backend.mysql.xa.XAStateLog;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.sql.SQLException;

public class XAHandler extends AbstractXAHandler implements TransactionHandler {

    public XAHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void commit() {
        commit(null);
    }

    @Override
    public void commit(TransactionCallback transactionCallback) {
        this.transactionCallback = transactionCallback;
        if (session.getTargetCount() <= 0) {
            CommitStage commitStage = new CommitStage(session, null, this.transactionCallback);
            commitStage.next(false, null, null);
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
    public void syncImplicitCommit() throws SQLException {
        // implicit commit is not supported in XA transactions, so ignore
    }

    @Override
    public void rollback() {
        rollback(null);
    }

    @Override
    public void rollback(TransactionCallback transactionCallback) {
        this.transactionCallback = transactionCallback;
        if (session.getTargetCount() <= 0) {
            RollbackStage rollbackStage = new RollbackStage(session, null, this.transactionCallback);
            rollbackStage.next(false, null, null);
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
            changeStageTo(new XARollbackStage(session, this));
            return;
        }
        interruptTx = false;
        changeStageTo(next());
    }

    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        this.packetIfSuccess = previousSendData;
        this.interruptTx = false;
    }

    private void initXALogEntry() {
        CoordinatorLogEntry coordinatorLogEntry = new CoordinatorLogEntry(session.getSessionXaID(),
                new ParticipantLogEntry[session.getTargetCount()], TxState.TX_STARTED_STATE);
        XAStateLog.flushMemoryRepository(session.getSessionXaID(), coordinatorLogEntry);

        int position = 0;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);

            XAStateLog.initRecoveryLog(session.getSessionXaID(), position, conn.getBackendService());
            position++;
        }
    }
}
