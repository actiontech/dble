package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAPrepareStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XAPrepareStage.class);

    public XAPrepareStage(XATransactionContext context) {
        super(context);
    }

    @Override
    public XAStage next(boolean isFail) {
        //
        if (isFail) return new XARollbackStage(context, false);
        return new XACommitStage(context);
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(context.getSessionXaId(), TxState.TX_PREPARING_STATE)) {
            super.onEnterStage();
        } else {
            context.getHandler().setFail("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(context.getSessionXaId(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA PREPARE " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA PREPARE " + xaTxId);
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_PREPARED_STATE);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectionClose(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }
}
