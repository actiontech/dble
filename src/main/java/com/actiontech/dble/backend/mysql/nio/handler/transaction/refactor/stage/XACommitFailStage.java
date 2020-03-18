package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XACommitFailStage extends XACommitStage {

    private static Logger logger = LoggerFactory.getLogger(XACommitFailStage.class);

    public XACommitFailStage(NonBlockingSession session) {
        super(session);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (!isFail) {
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMITTED_STATE);
            return null;
        }

        if (getXaContext().getRetryTimes() <= AUTO_RETRY_TIMES) {
            // try commit several times
            logger.warn("fail to COMMIT xa transaction " + session.getSessionXaID() + " at the " + getXaContext().getRetryTimes() + "th time!");
            return this;
        }

        // close this session ,add to schedule job
        session.getSource().close("COMMIT FAILED but it will try to COMMIT repeatedly in background until it is success!");
        // kill xa or retry to commit xa in background
        final int count = DbleServer.getInstance().getConfig().getSystem().getXaRetryCount();
        if (!session.isRetryXa()) {
            String warnStr = "kill xa session by manager cmd!";
            logger.warn(warnStr);
            session.forceClose(warnStr);
        } else if (count == 0 || getXaContext().getBackgroundRetryTimes() <= count) {
            String warnStr = "fail to COMMIT xa transaction " + session.getSessionXaID() + " at the " + getXaContext().getBackgroundRetryTimes() + "th time in background!";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", session.getSessionXaID()));

            XaDelayProvider.beforeAddXaToQueue(count, session.getSessionXaID());
            XASessionCheck.getInstance().addCommitSession(session);
            XaDelayProvider.afterAddXaToQueue(count, session.getSessionXaID());
        }
        return null;
    }

    @Override
    public void onEnterStage() {
        getXaContext().incrRetryTimes();
        XAStateLog.saveXARecoveryLog(session.getSessionXaID(), TxState.TX_COMMIT_FAILED_STATE);
        super.onEnterStage();
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        if (conn.getXaStatus() != TxState.TX_COMMIT_FAILED_STATE) {
            session.releaseConnection(rrn, true, false);
            return;
        }
        MySQLConnection newConn = session.freshConn(conn, getXaContext().getHandler());
        if (newConn.equals(conn)) {
            getXaContext().getHandler().fakedResponse(conn);
        } else {
            getXaContext().getXaOldThreadIds().putIfAbsent(conn.getAttachment(), conn.getThreadId());
            String xaTxId = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XaDelayProvider.delayBeforeXaCommit(rrn.getName(), xaTxId);
            newConn.execCmd("XA COMMIT " + xaTxId);
        }
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xid = conn.getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, conn.getSchema(), rrn.getName(), conn.getPool().getDbPool().getSource());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                // Unknown XID ,if xa transaction only contains select statement, xid will lost after restart server although prepared
                conn.setXaStatus(TxState.TX_COMMITTED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
            } else {
                if (handler.isExistXid()) {
                    // kill mysql connection holding xa transaction, so current xa transaction can be committed next time.
                    handler.killXaThread(getXaContext().getXaOldThreadIds().get(conn.getAttachment()));
                }
                conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
                XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
            }
        } else {
            conn.setXaStatus(TxState.TX_COMMIT_FAILED_STATE);
            XAStateLog.saveXARecoveryLog(session.getSessionXaID(), conn);
        }
    }

}
