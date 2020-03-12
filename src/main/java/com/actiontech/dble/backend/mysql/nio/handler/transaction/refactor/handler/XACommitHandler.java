/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.handler;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.CommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XAEndStage;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.server.NonBlockingSession;

public class XACommitHandler extends AbstractXAHandler implements CommitNodesHandler {

    public XACommitHandler(NonBlockingSession session) {
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

        changeStageTo(new XAEndStage(context));
    }

    @Override
    public void setImplicitCommitHandler(ImplicitCommitHandler handler) {

    }

}
