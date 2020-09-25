package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class XAStage implements TransactionStage {
    protected static final Logger LOGGER = LoggerFactory.getLogger(XAStage.class);

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

    public abstract void onEnterStage(MySQLResponseService conn);

    @Override
    public void onEnterStage() {
        xaHandler.setUnResponseRrns();
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            if (session.getTarget(rrn).getBackendService() != null) {
                onEnterStage(session.getTarget(rrn).getBackendService());
            } else {
                xaHandler.fakedResponse(rrn);
                session.releaseConnection(rrn, LOGGER.isDebugEnabled(), false);
            }
        }
    }

    protected void feedback(boolean isSuccess) {
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
        MySQLPacket sendData = xaHandler.getPacketIfSuccess();
        if (sendData != null) {
            sendData.write(session.getSource());
        } else {
            session.getShardingService().writeOkPacket();
        }
        xaHandler.clearResources();
    }

    // return ok
    public abstract void onConnectionOk(MySQLResponseService service);

    // connect error
    public abstract void onConnectionError(MySQLResponseService service, int errNo);

    // connect close
    public abstract void onConnectionClose(MySQLResponseService service);

    // connect error
    public abstract void onConnectError(MySQLResponseService service);

    public abstract String getStage();
}
