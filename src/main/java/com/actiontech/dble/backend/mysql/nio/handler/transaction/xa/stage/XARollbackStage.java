package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
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
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
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
    public void onEnterStage(MySQLResponseService service) {
        TxState state = service.getXaStatus();
        RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
        // if conn is closed or has been rollback, release conn
        if (state == TxState.TX_INITIALIZE_STATE || state == TxState.TX_CONN_QUIT ||
                state == TxState.TX_ROLLBACKED_STATE || (lastStageIsXAEnd && service.getConnection().isClosed())) {
            xaHandler.fakedResponse(service, null);
            session.releaseConnection(rrn, logger.isDebugEnabled(), false);
            return;
        }

        // need fresh conn to rollback again
        if (state == TxState.TX_PREPARE_UNCONNECT_STATE || state == TxState.TX_ROLLBACK_FAILED_STATE ||
                (!lastStageIsXAEnd && service.getConnection().isClosed())) {
            MySQLResponseService newService = session.freshConn(service.getConnection(), xaHandler);
            xaOldThreadIds.putIfAbsent(service.getAttachment(), service.getConnection().getThreadId());
            if (newService.equals(service)) {
                xaHandler.fakedResponse(service, "fail to fresh connection to rollback failed xa transaction");
                return;
            }
            service = newService;
        }
        String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA ROLLBACK " + xaTxId + " to " + service);
        }
        service.execCmd("XA ROLLBACK " + xaTxId + ";");
    }

    @Override
    public void onConnectionOk(MySQLResponseService service) {
        xaOldThreadIds.remove(service.getAttachment());
        service.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
        service.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLResponseService service, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
            String xid = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, service.getConnection().getSchema(), rrn.getName(),
                    ((PhysicalDbInstance) service.getConnection().getPoolRelated().getInstance()).getDbGroup().getWriteDbInstance());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            handler.killXaThread(xaOldThreadIds.get(rrn));

            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                xaOldThreadIds.remove(rrn);
                service.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
                service.setXaStatus(TxState.TX_INITIALIZE_STATE);
            }
        } else if (lastStageIsXAEnd) {
            service.getConnection().businessClose("rollback error");
            service.setXaStatus(TxState.TX_ROLLBACKED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
        } else {
            service.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
        }
    }

    @Override
    public void onConnectionClose(MySQLResponseService service) {
        if (lastStageIsXAEnd) {
            service.getConnection().businessClose("conn has been closed");
            service.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            service.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectError(MySQLResponseService service) {
        if (lastStageIsXAEnd) {
            service.getConnection().businessClose("conn connect error");
            service.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        } else {
            service.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        }
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public String getStage() {
        return ROLLBACK_STAGE;
    }

    protected TxState getSaveLogTxState() {
        return TxState.TX_ROLLBACKING_STATE;
    }
}
