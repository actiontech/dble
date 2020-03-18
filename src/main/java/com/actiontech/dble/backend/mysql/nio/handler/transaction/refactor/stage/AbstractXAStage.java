package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public abstract class AbstractXAStage implements XAStage {

    static final int AUTO_RETRY_TIMES = 5;
    protected NonBlockingSession session;

    AbstractXAStage(NonBlockingSession session) {
        this.session = session;
    }

    public abstract void onEnterStage(MySQLConnection conn);

    @Override
    public void onEnterStage() {
        XATransactionContext context = session.getXaContext();
        context.getHandler().setUnResponseRrns();
        session.setDiscard(false);
        for (RouteResultsetNode rrn : context.getInvolvedRrns()) {
            onEnterStage((MySQLConnection) context.getSession().getTarget(rrn));
        }
        session.setDiscard(true);
    }

    protected XATransactionContext getXaContext() {
        return session.getXaContext();
    }

}
