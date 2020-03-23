package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XACommitStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XACommitStage.class);

    public XACommitStage(NonBlockingSession session, AbstractXAHandler handler) {
        super(session, handler);
    }

    @Override
    public XAStage next(boolean isFail, String errMsg, byte[] errPacket) {
        if (isFail) {
            return new XACommitFailStage(session, xaHandler);
        }
        // success
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTED_STATE);
        feedback(true);
        return null;
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), getSaveLogTxState())) {
            super.onEnterStage();
        } else {
            xaHandler.interruptTx("saveXARecoveryLog error, the stage is " + getSaveLogTxState());
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        if (conn.isClosed()) {
            conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
            xaHandler.fakedResponse(conn, "the conn has been closed before executing XA COMMIT");
        } else {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (logger.isDebugEnabled()) {
                logger.debug("XA COMMIT " + xaTxId + " to " + conn);
            }
            XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
            conn.execCmd("XA COMMIT " + xaTxId);
        }
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

    @Override
    public String getStage() {
        return COMMIT_STAGE;
    }

    protected TxState getSaveLogTxState() {
        return TxState.TX_COMMITTING_STATE;
    }

}
