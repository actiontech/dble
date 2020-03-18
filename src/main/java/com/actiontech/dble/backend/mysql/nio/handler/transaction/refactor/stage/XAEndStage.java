package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAEndStage extends AbstractXAStage {

    private static Logger logger = LoggerFactory.getLogger(XAEndStage.class);

    public XAEndStage(NonBlockingSession session) {
        super(session);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (isFail) return null;
        // commit: end -> prepare，end error: -> rollback
        // rollback: end -> rollback
        if (getXaContext().isRollback()) {
            return new XARollbackStage(session, true);
        }
        return new XAPrepareStage(session);
    }

    @Override
    public void onEnterStage() {
        getXaContext().initXALogEntry();
        super.onEnterStage();
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
        if (conn.isClosed()) {
            conn.setXaStatus(TxState.TX_CONN_QUIT);
            getXaContext().getHandler().fakedResponse(conn);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("XA END " + xaTxId + " to " + conn);
            }
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

}
