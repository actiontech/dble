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

public class XAEndStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XAEndStage.class);
    private volatile boolean isRollback;

    public XAEndStage(NonBlockingSession session, AbstractXAHandler handler, boolean isRollback) {
        super(session, handler);
        this.isRollback = isRollback;
    }

    public void setRollback(boolean rollback) {
        isRollback = rollback;
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, byte[] errPacket) {
        if (isRollback) {
            return new XARollbackStage(session, xaHandler, true);
        }

        if (isFail) {
            if (xaHandler.isInterruptTx()) {
                session.getSource().setTxInterrupt(errMsg);
                session.getSource().write(errPacket);
                return null;
            } else {
                return new XARollbackStage(session, xaHandler, true);
            }
        }

        return new XAPrepareStage(session, xaHandler);
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        if (conn.isClosed()) {
            conn.setXaStatus(TxState.TX_CONN_QUIT);
            xaHandler.fakedResponse(conn, "the conn has been closed before executing XA END");
        } else {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (logger.isDebugEnabled()) {
                logger.debug("XA END " + xaTxId + " to " + conn);
            }
            XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
            conn.execCmd("XA END " + xaTxId);
        }
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_ENDED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public String getStage() {
        return END_STAGE;
    }

}
