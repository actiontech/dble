package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public abstract class XAStage implements TransactionStage {

    public static final String END_STAGE = "XA END STAGE";
    public static final String PREPARE_STAGE = "XA PREPARE STAGE";
    public static final String COMMIT_STAGE = "XA COMMIT STAGE";
    public static final String COMMIT_FAIL_STAGE = "XA COMMIT FAIL STAGE";
    public static final String ROLLBACK_STAGE = "XA ROLLBACK STAGE";
    public static final String ROLLBACK_FAIL_STAGE = "XA ROLLBACK FAIL STAGE";

    protected final NonBlockingSession session;
    protected AbstractXAHandler xaHandler;

    XAStage(NonBlockingSession session, AbstractXAHandler handler) {
        this.session = session;
        this.xaHandler = handler;
    }

    public abstract void onEnterStage(MySQLConnection conn);

    @Override
    public void onEnterStage() {
        session.setDiscard(false);
        xaHandler.setUnResponseRrns();
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            onEnterStage((MySQLConnection) session.getTarget(rrn));
        }
        session.setDiscard(true);
    }

    protected void feedback(boolean isSuccess) {
        session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
        session.clearResources(false);
        if (session.closed()) {
            return;
        }

        if (isSuccess) {
            session.setFinishedCommitTime();
            ImplicitCommitHandler implicitCommitHandler = xaHandler.getImplicitCommitHandler();
            if (implicitCommitHandler != null) {
                xaHandler.clearResources();
                implicitCommitHandler.next();
                return;
            }
        }
        session.setResponseTime(isSuccess);
        byte[] sendData = xaHandler.getPacketIfSuccess();
        if (sendData != null) {
            session.getSource().write(sendData);
        } else {
            session.getSource().write(session.getOkByteArray());
        }
        xaHandler.clearResources();
    }

    // return ok
    public abstract void onConnectionOk(MySQLConnection conn);

    // connect error
    public abstract void onConnectionError(MySQLConnection conn, int errNo);

    // connect close
    public abstract void onConnectionClose(MySQLConnection conn);

    // connect error
    public abstract void onConnectError(MySQLConnection conn);

    public abstract String getStage();
}
