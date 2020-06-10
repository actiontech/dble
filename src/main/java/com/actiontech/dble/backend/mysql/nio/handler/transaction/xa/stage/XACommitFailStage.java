package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class XACommitFailStage extends XACommitStage {

    private static Logger logger = LoggerFactory.getLogger(XACommitFailStage.class);
    private static final int AUTO_RETRY_TIMES = 5;
    private ConcurrentMap<Object, Long> xaOldThreadIds;

    private AtomicInteger retryTimes = new AtomicInteger(1);
    private AtomicInteger backgroundRetryTimes = new AtomicInteger(0);
    private int backgroundRetryCount = SystemConfig.getInstance().getXaRetryCount();

    public XACommitFailStage(NonBlockingSession session, AbstractXAHandler handler) {
        super(session, handler);
        this.xaOldThreadIds = new ConcurrentHashMap<>(session.getTargetCount());
    }

    @Override
    public XAStage next(boolean isFail, String errMsg, byte[] errPacket) {
        String xaId = session.getSessionXaID();
        if (!isFail || xaOldThreadIds.isEmpty()) {
            XAStateLog.saveXARecoveryLog(xaId, TxState.TX_COMMITTED_STATE);
            // remove session in background
            XASessionCheck.getInstance().getCommittingSession().remove(session.getSource().getId());
            // resolve alert
            AlertUtil.alertSelfResolve(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("XA_ID", session.getSessionXaID()));
            feedback(true);
            return null;
        }

        if (SystemConfig.getInstance().getUseSerializableMode() == 1 || retryTimes.get() < AUTO_RETRY_TIMES) {
            // try commit several times
            logger.warn("fail to COMMIT xa transaction " + xaId + " at the " + retryTimes + "th time!");
            XaDelayProvider.beforeInnerRetry(retryTimes.incrementAndGet(), xaId);
            return this;
        }

        // close this session ,add to schedule job
        if (!session.closed()) {
            session.getSource().close("COMMIT FAILED but it will try to COMMIT repeatedly in background until it is success!");
        }
        // kill xa or retry to commit xa in background
        if (!session.isRetryXa()) {
            String warnStr = "kill xa session by manager cmd!";
            logger.warn(warnStr);
            session.forceClose(warnStr);
            return null;
        }

        if (backgroundRetryCount == 0 || backgroundRetryTimes.incrementAndGet() <= backgroundRetryCount) {
            String warnStr = "fail to COMMIT xa transaction " + xaId + " at the " + backgroundRetryTimes + "th time in background!";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", xaId));

            XaDelayProvider.beforeAddXaToQueue(backgroundRetryTimes.get(), xaId);
            XASessionCheck.getInstance().addCommitSession(session);
            XaDelayProvider.afterAddXaToQueue(backgroundRetryTimes.get(), xaId);
        }
        return null;
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        if (!conn.isClosed() && conn.getXaStatus() != TxState.TX_COMMIT_FAILED_STATE) {
            xaHandler.fakedResponse(conn, null);
            session.releaseConnection(rrn, true, false);
            return;
        }
        MySQLConnection newConn = session.freshConn(conn, xaHandler);
        xaOldThreadIds.putIfAbsent(conn.getAttachment(), conn.getThreadId());
        if (newConn.equals(conn)) {
            xaHandler.fakedResponse(conn, "fail to fresh connection to commit failed xa transaction");
        } else {
            String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
            newConn.execCmd("XA COMMIT " + xaTxId);
        }
    }

    @Override
    public void onConnectionOk(MySQLConnection conn) {
        xaOldThreadIds.remove(conn.getAttachment());
        super.onConnectionOk(conn);
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xid = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, conn.getSchema(), rrn.getName(), conn.getDbInstance().getDbGroup().getWriteDbInstance());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                // Unknown XID ,if xa transaction only contains select statement, xid will lost after restart server although prepared
                xaOldThreadIds.remove(rrn);
                conn.setXaStatus(TxState.TX_COMMITTED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
                conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
            } else {
                if (handler.isExistXid()) {
                    // kill mysql connection holding xa transaction, so current xa transaction can be committed next time.
                    handler.killXaThread(xaOldThreadIds.get(conn.getAttachment()));
                }
                conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
            }
        } else {
            conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        }
    }

    @Override
    public String getStage() {
        return COMMIT_FAIL_STAGE;
    }

    @Override
    protected TxState getSaveLogTxState() {
        return TxState.TX_COMMIT_FAILED_STATE;
    }

}
