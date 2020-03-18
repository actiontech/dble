package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XACommitStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XACommitStage.class);

    public XACommitStage(NonBlockingSession session) {
        super(session);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail) {
            return new XACommitFailStage(session);
        }
        // success
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTED_STATE);
        return null;
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTING_STATE)) {
            super.onEnterStage();
        } else {
            getXaContext().getHandler().setFail("saveXARecoveryLog error, the stage is TX_COMMITTING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA COMMIT " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA COMMIT " + xaTxId);
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_COMMITTED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }
}
