package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
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
                session.getShardingService().setTxInterrupt(errMsg);
                errPacket.setPacketId(session.getShardingService().nextPacketId());
                errPacket.write(session.getSource());
                return null;
            }
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
            RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
            String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (logger.isDebugEnabled()) {
                logger.debug("XA PREPARE " + xaTxId + " to " + service);
            }
            XaDelayProvider.delayBeforeXaPrepare(rrn.getName(), xaTxId);
            service.execCmd("XA PREPARE " + xaTxId);
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
