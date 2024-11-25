/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.oceanbase.obsharding_d.backend.mysql.xa.TxState;
import com.oceanbase.obsharding_d.backend.mysql.xa.XAStateLog;
import com.oceanbase.obsharding_d.btrace.provider.XaDelayProvider;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
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
            try {
                RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
                String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
                if (logger.isDebugEnabled()) {
                    logger.debug("XA COMMIT " + xaTxId + " to " + service);
                }
                XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
                service.execCmd("XA COMMIT " + xaTxId);
            } catch (Exception e) {
                logger.info("xa commit error", e);
                if (!xaHandler.isFail()) {
                    xaHandler.fakedResponse(service, "cause error when executing XA COMMIT. reason [" + e.getMessage() + "]");
                } else {
                    xaHandler.fakedResponse(service, null);
                }
            }
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
