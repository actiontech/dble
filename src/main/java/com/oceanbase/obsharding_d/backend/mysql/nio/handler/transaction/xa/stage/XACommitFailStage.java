/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.XAAnalysisHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.oceanbase.obsharding_d.backend.mysql.xa.TxState;
import com.oceanbase.obsharding_d.backend.mysql.xa.XAStateLog;
import com.oceanbase.obsharding_d.btrace.provider.XaDelayProvider;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

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
    public XAStage next(boolean isFail, String errMsg, MySQLPacket errPacket) {
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
            if (xaHandler.getTransactionCallback() != null) {
                xaHandler.getTransactionCallback().callback();
            }
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
        } else {
            String warnStr = "The background retry mechanism for xa transaction " + xaId + " stops when the threshold[" + backgroundRetryCount + "] is reached.";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_STOP, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", xaId));
        }
        return null;
    }

    @Override
    public void onEnterStage(MySQLResponseService service) {
        RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
        if (!service.getConnection().isClosed() && service.getXaStatus() != TxState.TX_COMMIT_FAILED_STATE) {
            session.releaseConnection(rrn, false);
            xaHandler.fakedResponse(rrn);
            return;
        }
        MySQLResponseService newService = session.freshConn(service.getConnection(), xaHandler);
        xaOldThreadIds.putIfAbsent(service.getAttachment(), service.getConnection().getThreadId());
        if (newService.equals(service)) {
            xaHandler.fakedResponse(service, "fail to fresh connection to commit failed xa transaction");
        } else {
            String xaTxId = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
            if (logger.isDebugEnabled()) {
                logger.debug("XA COMMIT " + xaTxId + " to " + service);
            }
            newService.execCmd("XA COMMIT " + xaTxId);
        }
    }

    @Override
    public void onConnectionOk(MySQLResponseService service) {
        xaOldThreadIds.remove(service.getAttachment());
        super.onConnectionOk(service);
    }

    @Override
    public void onConnectionError(MySQLResponseService service, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            // inner 1497
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
            String xid = service.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XAAnalysisHandler xaAnalysisHandler = new XAAnalysisHandler(
                    ((PhysicalDbInstance) service.getConnection().getPoolRelated().getInstance()).getDbGroup().getWriteDbInstance());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            boolean isExistXid = xaAnalysisHandler.isExistXid(xid);
            boolean isSuccess = xaAnalysisHandler.isSuccess();
            if (isSuccess && !isExistXid) {
                // Unknown XID ,if xa transaction only contains select statement, xid will lost after restart server although prepared
                xaOldThreadIds.remove(rrn);
                service.setXaStatus(TxState.TX_COMMITTED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
                service.setXaStatus(TxState.TX_INITIALIZE_STATE);
            } else {
                if (isExistXid) {
                    // kill mysql connection holding xa transaction, so current xa transaction can be committed next time.
                    xaAnalysisHandler.killThread(xaOldThreadIds.get(service.getAttachment()));
                }
                service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
            }
        } else {
            service.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), service);
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
