package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAPrepareStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XAPrepareStage.class);

    private volatile boolean prepareUnconnect = false;

    public XAPrepareStage(NonBlockingSession session) {
        super(session);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail) {
            if (prepareUnconnect) {
                return new XARollbackStage(session, false);
            }
            return null;
        }
        return new XACommitStage(session);
    }

    @Override
    public void onEnterStage() {
        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_PREPARING_STATE)) {
            super.onEnterStage();
        } else {
            getXaContext().getHandler().setFail("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaPrepare(rrn.getName(), xaTxId);
        if (logger.isDebugEnabled()) {
            logger.debug("XA PREPARE " + xaTxId + " to " + conn);
        }
        conn.execCmd("XA PREPARE " + xaTxId);
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        conn.setXaStatus(TxState.TX_PREPARED_STATE);
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
}
