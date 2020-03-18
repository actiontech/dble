package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XARollbackStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackStage.class);
    private boolean lastStageIsXAEnd;

    public XARollbackStage(NonBlockingSession session, boolean isFromEndStage) {
        super(session);
        this.lastStageIsXAEnd = isFromEndStage;
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail && !getXaContext().getXaOldThreadIds().isEmpty()) {
            return new XARollbackFailStage(session);
        }
        // success
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
        return null;
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKING_STATE)) {
            super.onEnterStage();
        } else {
            getXaContext().getHandler().setFail("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        TxState state = conn.getXaStatus();
        if (state == TxState.TX_CONN_QUIT || state == TxState.TX_ROLLBACKED_STATE) {
            getXaContext().getHandler().fakedResponse(conn);
            return;
        } else if (state == TxState.TX_PREPARE_UNCONNECT_STATE || state == TxState.TX_ROLLBACK_FAILED_STATE) {
            MySQLConnection newConn = session.freshConn(conn, getXaContext().getHandler());
            if (newConn.equals(conn)) {
                getXaContext().getHandler().fakedResponse(conn);
                return;
            }
            getXaContext().getXaOldThreadIds().putIfAbsent(conn.getAttachment(), conn.getThreadId());
            conn = newConn;
        }

        XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA ROLLBACK " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA ROLLBACK " + xaTxId + ";");
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xid = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, conn.getSchema(), rrn.getName(), conn.getPool().getDbPool().getSource());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            handler.killXaThread(getXaContext().getXaOldThreadIds().get(rrn));

            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                getXaContext().getXaOldThreadIds().remove(rrn);
                conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
                conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
            }
        } else if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        }
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

}
