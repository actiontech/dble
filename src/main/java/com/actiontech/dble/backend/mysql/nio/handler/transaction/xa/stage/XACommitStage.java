package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XACommitStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XACommitStage.class);

    public XACommitStage(NonBlockingSession session, AbstractXAHandler handler) {
        super(session, handler);
    }

    @Override
    public XAStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
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
    public void onEnterStage(MySQLResponseService service) {
        if (service.getConnection().isClosed()) {
            service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
            xaHandler.fakedResponse(service, "the conn has been closed before executing XA COMMIT");
        } else {
            RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
            String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (logger.isDebugEnabled()) {
                logger.debug("XA COMMIT " + xaTxId + " to " + service);
            }
            XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
            service.execCmd("XA COMMIT " + xaTxId);
        }
    }

    @Override
    public void onConnectionOk(MySQLResponseService service) {
        service.setXaStatus(TxState.TX_COMMITTED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
        service.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLResponseService service, int errNo) {
        service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectionClose(MySQLResponseService service) {
        service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectError(MySQLResponseService service) {
        service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public String getStage() {
        return COMMIT_STAGE;
    }

    protected TxState getSaveLogTxState() {
        return TxState.TX_COMMITTING_STATE;
    }

}
