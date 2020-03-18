/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.handler;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.TransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XAEndStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XARollbackStage;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.server.NonBlockingSession;

public class XAHandler extends AbstractXAHandler implements TransactionHandler {

    public XAHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void commit() {
        if (session.getTargetCount() <= 0) {
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(session.getIsMultiStatement().get());
            return;
        }

        if (session.getXaState() != null && session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }

        changeStageTo(new XAEndStage(session));
    }

    @Override
    public void setImplicitCommitHandler(ImplicitCommitHandler handler) {

    }

    @Override
    public void rollback() {
        if (session.getTargetCount() <= 0) {
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(session.getIsMultiStatement().get());
        }

        //get session's lock before sending rollback(in fact, after ended)
        //then the XA transaction will be not killed. if killed ,then we will not rollbacks
        if (currentStage instanceof XAEndStage) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
            changeStageTo(new XARollbackStage(session, true));
        } else {
            session.getXaContext().setRollback(true);
            changeStageTo(new XAEndStage(session));
        }
    }

}
