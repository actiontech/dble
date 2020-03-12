package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XARollbackStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackStage.class);
    private boolean lastStageIsXAEnd;


    public XARollbackStage(XATransactionContext context, boolean isFromEndStage) {
        super(context);
        this.lastStageIsXAEnd = isFromEndStage;
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail) {
            return new XARollbackFailStage(context);
        }
        // success
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), TxState.TX_ROLLBACKED_STATE);
        return null;
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(context.getSessionXaId(), TxState.TX_ROLLBACKING_STATE)) {
            super.onEnterStage();
        } else {
            context.getHandler().setFail("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(context.getSessionXaId(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaRollback(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA ROLLBACK " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA ROLLBACK " + xaTxId + ";");
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
        conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        }
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        }
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        if (lastStageIsXAEnd) {
            conn.setXaStatus(TxState.TX_ROLLBACK_FAILED_STATE);
        } else {
            conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
        }
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

}
