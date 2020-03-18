package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XARollbackFailStage extends XARollbackStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackFailStage.class);

    XARollbackFailStage(NonBlockingSession session) {
        super(session, false);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (!isFail) {
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACKED_STATE);
            return null;
        }

        if (getXaContext().getRetryTimes() <= AUTO_RETRY_TIMES) {
            // try commit several times
            logger.warn("fail to ROLLBACK xa transaction " + session.getSessionXaID() + " at the " + getXaContext().getRetryTimes() + "th time!");
            return this;
        }

        // close this session ,add to schedule job
        session.getSource().close("ROLLBACK FAILED but it will try to ROLLBACK repeatedly in background until it is success!");
        // kill xa or retry to commit xa in background
        final int count = DbleServer.getInstance().getConfig().getSystem().getXaRetryCount();
        if (!session.isRetryXa()) {
            String warnStr = "kill xa session by manager cmd!";
            logger.warn(warnStr);
            session.forceClose(warnStr);
        } else if (count == 0 || getXaContext().getBackgroundRetryTimes() <= count) {
            String warnStr = "fail to ROLLBACK xa transaction " + getXaContext().getSessionXaId() + " at the " + getXaContext().getBackgroundRetryTimes() + "th time in background!";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", getXaContext().getSessionXaId()));

            XaDelayProvider.beforeAddXaToQueue(count, session.getSessionXaID());
            XASessionCheck.getInstance().addCommitSession(session);
            XaDelayProvider.afterAddXaToQueue(count, session.getSessionXaID());
        }
        return null;
    }

    @Override
    public void onEnterStage() {
        getXaContext().incrRetryTimes();
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_ROLLBACK_FAILED_STATE);
        super.onEnterStage();
    }

}
