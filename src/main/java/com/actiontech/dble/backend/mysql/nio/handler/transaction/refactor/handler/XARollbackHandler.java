/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.handler;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.RollbackNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XAEndStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XARollbackStage;
import com.actiontech.dble.server.NonBlockingSession;

public class XARollbackHandler extends AbstractXAHandler implements RollbackNodesHandler {

    public XARollbackHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void rollback() {
        if (session.getTargetCount() <= 0) {
            session.getSource().write(session.getOkByteArray());
            session.multiStatementNextSql(session.getIsMultiStatement().get());
        }

        //get session's lock before sending rollback(in fact, after ended)
        //then the XA transaction will be not killed. if killed ,then we will not rollback
        if (currentStage instanceof XAEndStage) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
            changeStageTo(new XARollbackStage(context, true));
        } else {
            context.setRollback(true);
            changeStageTo(new XAEndStage(context));
        }
    }

}
