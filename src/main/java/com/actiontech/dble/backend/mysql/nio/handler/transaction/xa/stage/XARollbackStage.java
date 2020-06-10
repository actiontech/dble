package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class XARollbackStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackStage.class);
    private boolean lastStageIsXAEnd;
    protected ConcurrentMap<Object, Long> xaOldThreadIds;

    public XARollbackStage(NonBlockingSession session, AbstractXAHandler handler, boolean isFromEndStage) {
        super(session, handler);
        this.lastStageIsXAEnd = isFromEndStage;
        this.xaOldThreadIds = new ConcurrentHashMap<>(session.getTargetCount());
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, byte[] errPacket) {
        if (isFail && !xaOldThreadIds.isEmpty()) {
            return new XARollbackFailStage(session, xaHandler, lastStageIsXAEnd);
        }
        // success
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
        feedback(false);
        return null;
    }

    @Override
    public void onEnterStage() {
        if (lastStageIsXAEnd && session.closed()) {
            session.forceClose("front conn is closed when xa stage is in xa end");
            return;
        }

        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), getSaveLogTxState())) {
            super.onEnterStage();
        } else {
            xaHandler.interruptTx("saveXARecoveryLog error, the stage is " + getSaveLogTxState());
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        TxState state = conn.getXaStatus();
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        // if conn is closed or has been rollback, release conn
        if (state == TxState.TX_INITIALIZE_STATE || state == TxState.TX_CONN_QUIT ||
                state == TxState.TX_ROLLBACKED_STATE || (lastStageIsXAEnd && conn.isClosed())) {
            xaHandler.fakedResponse(conn, null);
            session.releaseConnection(rrn, logger.isDebugEnabled(), false);
            return;
        }

        // need fresh conn to rollback again
        if (state == TxState.TX_PREPARE_UNCONNECT_STATE || state == TxState.TX_ROLLBACK_FAILED_STATE ||
                (!lastStageIsXAEnd && conn.isClosed())) {
            MySQLConnection newConn = session.freshConn(conn, xaHandler);
            xaOldThreadIds.putIfAbsent(conn.getAttachment(), conn.getThreadId());
            if (newConn.equals(conn)) {
                xaHandler.fakedResponse(conn, "fail to fresh connection to rollback failed xa transaction");
                return;
            }
            conn = newConn;
        }
        String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA ROLLBACK " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA ROLLBACK " + xaTxId + ";");
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        xaOldThreadIds.remove(conn.getAttachment());
        conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xid = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, conn.getSchema(), rrn.getName(), conn.getDbInstance().getDbGroup().getWriteDbInstance());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            handler.killXaThread(xaOldThreadIds.get(rrn));

            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                xaOldThreadIds.remove(rrn);
                conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
                conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
            }
        } else if (lastStageIsXAEnd) {
            conn.closeWithoutRsp("rollback error");
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
            conn.closeWithoutRsp("conn has been closed");
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        if (lastStageIsXAEnd) {
            conn.closeWithoutRsp("conn connect error");
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
    }

    @Override
    public String getStage() {
        return ROLLBACK_STAGE;
    }

    protected TxState getSaveLogTxState() {
        return TxState.TX_ROLLBACKING_STATE;
    }
}
