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

public class XAEndStage extends XAStage {

    private static Logger logger = LoggerFactory.getLogger(XAEndStage.class);
    private volatile boolean isRollback;

    public XAEndStage(NonBlockingSession session, AbstractXAHandler handler, boolean isRollback) {
        super(session, handler);
        this.isRollback = isRollback;
    }

    public void setRollback(boolean rollback) {
        isRollback = rollback;
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
        if (isRollback) {
            return new XARollbackStage(session, xaHandler);
        }

        if (isFail) {
            if (xaHandler.isInterruptTx()) {
                // In transaction: expect to follow up by manually executing rollback
                session.getShardingService().setTxInterrupt(errMsg);
                errPacket.setPacketId(session.getShardingService().nextPacketId());
                errPacket.write(session.getSource());
                return null;
            } else {
                // Not in transaction, automatic rollback directly
                return new XARollbackStage(session, xaHandler);
            }
        }

        return new XAPrepareStage(session, xaHandler);
    }

    @Override
    public void onEnterStage(MySQLResponseService service) {
        if (service.getConnection().isClosed()) {
            service.setXaStatus(TxState.TX_CONN_QUIT);
            xaHandler.fakedResponse(service, "the conn has been closed before executing XA END");
        } else {
            try {
                RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
                String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
                if (logger.isDebugEnabled()) {
                    logger.debug("XA END " + xaTxId + " to " + service);
                }
                XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
                service.execCmd("XA END " + xaTxId);
            } catch (Exception e) {
                logger.info("xa end error", e);
                if (!xaHandler.isFail()) {
                    xaHandler.fakedResponse(service, "cause error when executing XA END. reason [" + e.getMessage() + "]");
                } else {
                    xaHandler.fakedResponse(service, null);
                }


            }
        }
    }

    @Override
    public void onConnectionOk(MySQLResponseService service) {
        service.setXaStatus(TxState.TX_ENDED_STATE);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectionError(MySQLResponseService service, int errNo) {
        service.getConnection().businessClose("conn error");
        service.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectionClose(MySQLResponseService service) {
        service.getConnection().businessClose("conn has been closed");
        service.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public void onConnectError(MySQLResponseService service) {
        service.getConnection().businessClose("conn connect error");
        service.setXaStatus(TxState.TX_CONN_QUIT);
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
    }

    @Override
    public String getStage() {
        return END_STAGE;
    }

}
