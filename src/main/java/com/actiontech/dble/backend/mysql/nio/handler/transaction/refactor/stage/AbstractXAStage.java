package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public abstract class AbstractXAStage implements XAStage {

    static final int AUTO_RETRY_TIMES = 5;
    protected XATransactionContext context;

    AbstractXAStage(XATransactionContext context) {
        this.context = context;
    }

    public void onEnterStage(MySQLConnection conn) {
    }

    @Override
    public void onEnterStage() {
        context.getHandler().setUnResponseRrns();
        NonBlockingSession session = context.getSession();
        session.setDiscard(false);
        for (RouteResultsetNode rrn : context.getInvolvedRrns()) {
            onEnterStage((MySQLConnection) context.getSession().getTarget(rrn));
        }
        session.setDiscard(true);
    }

}
