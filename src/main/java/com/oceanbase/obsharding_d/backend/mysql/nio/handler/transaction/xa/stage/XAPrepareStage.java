/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionStage;
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

public class XAPrepareStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XAPrepareStage.class);

    private volatile boolean prepareUnconnect = false;

    XAPrepareStage(NonBlockingSession session, AbstractXAHandler handler) {
        super(session, handler);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
        if (isFail) {
            if (prepareUnconnect) {
                errPacket.setPacketId(session.getShardingService().nextPacketId());
                xaHandler.setPacketIfSuccess(errPacket);
            } else if (xaHandler.isInterruptTx()) {
                // In transaction: expect to follow up by manually executing rollback
                session.getShardingService().setTxInterrupt(errMsg);
                errPacket.setPacketId(session.getShardingService().nextPacketId());
                errPacket.write(session.getSource());
                return null;
            }
            // Not in transaction, automatic rollback directly
            return new XARollbackStage(session, xaHandler, false);
        }
        return new XACommitStage(session, xaHandler);
    }

    @Override
    public void onEnterStage() {
        if (session.closed()) {
            session.forceClose("front conn is closed when xa stage is in xa end");
            return;
        }

        if (XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_PREPARING_STATE)) {
            super.onEnterStage();
        } else {
            xaHandler.interruptTx("saveXARecoveryLog error, the stage is TX_PREPARING_STATE");
        }
    }

    @Override
    public void onEnterStage(MySQLResponseService service) {
        if (service.getConnection().isClosed()) {
            service.setXaStatus(TxState.TX_CONN_QUIT);
            xaHandler.fakedResponse(service, "the conn has been closed before executing XA PREPARE");
        } else {
            try {
                RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
                String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
                if (logger.isDebugEnabled()) {
                    logger.debug("XA PREPARE " + xaTxId + " to " + service);
                }
                XaDelayProvider.delayBeforeXaPrepare(rrn.getName(), xaTxId);
                service.execCmd("XA PREPARE " + xaTxId);
            } catch (Exception e) {
                logger.info("xa prepare error", e);
                if (!xaHandler.isFail()) {
                    xaHandler.fakedResponse(service, "cause error when executing XA PREPARE. reason [" + e.getMessage() + "]");
                } else {
                    xaHandler.fakedResponse(service, null);
                }
            }
        }
    }

    @Override
    public void onConnectionOk(MySQLResponseService service) {
        service.setXaStatus(TxState.TX_PREPARED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectionError(MySQLResponseService service, int errNo) {
        service.getConnection().close("prepare error");
        service.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectionClose(MySQLResponseService service) {
        prepareUnconnect = true;
        service.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectError(MySQLResponseService service) {
        prepareUnconnect = true;
        service.setXaStatus(TxState.TX_PREPARE_UNCONNECT_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public String getStage() {
        return PREPARE_STAGE;
    }
}
