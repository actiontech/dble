package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class XARollbackFailStage extends XARollbackStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackFailStage.class);
    private static final int AUTO_RETRY_TIMES = 5;

    private AtomicInteger retryTimes = new AtomicInteger(1);
    private AtomicInteger backgroundRetryTimes = new AtomicInteger(0);
    private int backgroundRetryCount = SystemConfig.getInstance().getXaRetryCount();

    public XARollbackFailStage(NonBlockingSession session, AbstractXAHandler handler, boolean isFromEndStage) {
        super(session, handler, isFromEndStage);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
        String xaId = session.getSessionXaID();
        if (!isFail || xaOldThreadIds.isEmpty()) {
            XAStateLog.saveXARecoveryLog(xaId, TxState.TX_ROLLBACKED_STATE);
            // remove session in background
            XASessionCheck.getInstance().getRollbackingSession().remove(session.getSource().getId());
            // resolve alert
            AlertUtil.alertSelfResolve(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("XA_ID", xaId));
            feedback(false);
            return null;
        }

        if (retryTimes.get() < AUTO_RETRY_TIMES) {
            // try commit several times
            logger.warn("fail to ROLLBACK xa transaction " + session.getSessionXaID() + " at the " + retryTimes + "th time!");
            XaDelayProvider.beforeInnerRetry(retryTimes.incrementAndGet(), xaId);
            return this;
        }

        // close the session ,add to schedule job
        if (!session.closed()) {
            StringBuilder closeReason = new StringBuilder("ROLLBACK FAILED but it will try to ROLLBACK repeatedly in background until it is success");
            if (errMsg != null) {
                closeReason.append(", the ERROR is ");
                closeReason.append(errMsg);
            }
            session.getSource().close(closeReason.toString());
        }

        // kill xa or retry to commit xa in background
        if (!session.isRetryXa()) {
            String warnStr = "kill xa session by manager cmd!";
            logger.warn(warnStr);
            session.forceClose(warnStr);
            return null;
        }

        if (backgroundRetryCount == 0 || backgroundRetryTimes.incrementAndGet() <= backgroundRetryCount) {
            String warnStr = "fail to ROLLBACK xa transaction " + session.getSessionXaID() + " at the " + backgroundRetryTimes + "th time in background!";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", xaId));

            XaDelayProvider.beforeAddXaToQueue(backgroundRetryTimes.get(), xaId);
            XASessionCheck.getInstance().addRollbackSession(session);
            XaDelayProvider.afterAddXaToQueue(backgroundRetryTimes.get(), xaId);
        }
        return null;
    }

    @Override
    public String getStage() {
        return ROLLBACK_FAIL_STAGE;
    }

    @Override
    protected TxState getSaveLogTxState() {
        return TxState.TX_ROLLBACK_FAILED_STATE;
    }
}
