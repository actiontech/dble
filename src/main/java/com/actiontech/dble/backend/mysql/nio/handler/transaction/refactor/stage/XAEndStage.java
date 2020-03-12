package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAEndStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XAEndStage.class);

    public XAEndStage(XATransactionContext context) {
        super(context);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail) return null;
        // commit: end -> prepare
        // rollback: end -> rollback
        // end error: -> rollback
        if (context.isRollback()) {
            return new XARollbackStage(context, true);
        }
        return new XAPrepareStage(context);
    }

    @Override
    public void onEnterStage() {
        context.initXALogEntry();
        super.onEnterStage();
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(context.getSessionXaId(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA END " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA END " + xaTxId);
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_ENDED_STATE);
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
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

    @Override
    public void onConnectError(MySQLConnection conn) {
        conn.close();
        conn.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
    }

}
