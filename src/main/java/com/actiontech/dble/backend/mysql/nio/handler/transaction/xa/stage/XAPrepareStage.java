package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAPrepareStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XAPrepareStage.class);

    private volatile boolean prepareUnconnect = false;

    XAPrepareStage(NonBlockingSession session, AbstractXAHandler handler) {
        super(session, handler);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, byte[] errPacket) {
        if (isFail) {
            if (prepareUnconnect) {
                xaHandler.setPacketIfSuccess(errPacket);
            } else if (xaHandler.isInterruptTx()) {
                session.getSource().setTxInterrupt(errMsg);
                session.getSource().write(errPacket);
                return null;
            }
            return new XARollbackStage(session, xaHandler, false);
        }
        return new XACommitStage(session, xaHandler);
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_PREPARING_STATE)) {
            super.onEnterStage();
        } else {
            xaHandler.interruptTx("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        if (conn.isClosed()) {
            conn.setXaStatus(TxState.TX_CONN_QUIT);
            xaHandler.fakedResponse(conn, "the conn has been closed before executing XA PREPARE");
        } else {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (logger.isDebugEnabled()) {
                logger.debug("XA PREPARE " + xaTxId + " to " + conn);
            }
            XaDelayProvider.delayBeforeXaPrepare(rrn.getName(), xaTxId);
            conn.execCmd("XA PREPARE " + xaTxId);
        }
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_PREPARED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        conn.close("prepare error");
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        prepareUnconnect = true;
        conn.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        prepareUnconnect = true;
        conn.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public String getStage() {
        return PREPARE_STAGE;
    }
}
