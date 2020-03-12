package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XACheckHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.backend.mysql.xa.XAStateLog;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.singleton.XASessionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XARollbackFailStage extends XARollbackStage {

    private static Logger logger = LoggerFactory.getLogger(XARollbackFailStage.class);

    XARollbackFailStage(XATransactionContext context) {
        super(context, false);
    }

    @Override
    public XAStage next(boolean isFail) {
        if (!isFail) {
            XAStateLog.saveXARecoveryLog(context.getSessionXaId(), TxState.TX_ROLLBACKED_STATE);
            return null;
        }

        if (context.getRetryTimes() <= AUTO_RETRY_TIMES) {
            // try commit several times
            logger.warn("fail to ROLLBACK xa transaction " + context.getSessionXaId() + " at the " + context.getRetryTimes() + "th time!");
            return this;
        }

        // close this session ,add to schedule job
        context.getSession().getSource().close("ROLLBACK FAILED but it will try to ROLLBACK repeatedly in background until it is success!");
        // kill xa or retry to commit xa in background
        final int count = DbleServer.getInstance().getConfig().getSystem().getXaRetryCount();
        if (!context.getSession().isRetryXa()) {
            String warnStr = "kill xa session by manager cmd!";
            logger.warn(warnStr);
            context.getSession().forceClose(warnStr);
        } else if (count == 0 || context.getBackgroundRetryTimes() <= count) {
            String warnStr = "fail to ROLLBACK xa transaction " + context.getSessionXaId() + " at the " + context.getBackgroundRetryTimes() + "th time in background!";
            logger.warn(warnStr);
            AlertUtil.alertSelf(AlarmCode.XA_BACKGROUND_RETRY_FAIL, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("XA_ID", context.getSessionXaId()));

            XaDelayProvider.beforeAddXaToQueue(count, context.getSessionXaId());
            XASessionCheck.getInstance().addCommitSession(context.getSession());
            XaDelayProvider.afterAddXaToQueue(count, context.getSessionXaId());
        }
        return null;
    }

    @Override
    public void onEnterStage() {
        context.incrRetryTimes();
        XAStateLog.saveXARecoveryLog(context.getSessionXaId(), TxState.TX_ROLLBACK_FAILED_STATE);
        super.onEnterStage();
    }

    @Override
    public void onEnterStage(MySQLConnection conn) {
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        String xaTxId = conn.getConnXID(context.getSessionXaId(), rrn.getMultiplexNum().longValue());
        XaDelayProvider.delayBeforeXaEnd(rrn.getName(), xaTxId);
        if (conn.getXaStatus() != TxState.TX_ROLLBACK_FAILED_STATE) {
            context.getSession().releaseConnection(rrn, true, false);
            return;
        }
        MySQLConnection newConn = context.getSession().freshConn(conn, context.getHandler());
        if (!newConn.equals(conn)) {
            //                xaOldThreadIds.putIfAbsent(mysqlCon.getAttachment(), mysqlCon.getThreadId());
            if (logger.isDebugEnabled()) {
                logger.debug("XA ROLLBACK " + xaTxId + " to " + conn);
            }
            newConn.execCmd("XA ROLLBACK " + xaTxId);
        } else {
            // fake response
            context.getHandler().fakedResponse(conn);
        }
    }

    @Override
    public void onConnectionError(MySQLConnection conn, int errNo) {
        if (errNo == ErrorCode.ER_XAER_NOTA) {
            RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
            String xid = conn.getConnXID(context.getSessionXaId(), rrn.getMultiplexNum().longValue());
            XACheckHandler handler = new XACheckHandler(xid, conn.getSchema(), rrn.getName(), conn.getPool().getDbPool().getSource());
            // if mysql connection holding xa transaction wasn't released, may result in ER_XAER_NOTA.
            // so we need check xid here
            // handler.killXaThread(xaOldThreadIds.get(rrn));

            handler.checkXid();
            if (handler.isSuccess() && !handler.isExistXid()) {
                //ERROR 1397 (XAE04): XAER_NOTA: Unknown XID, not prepared
                conn.setXaStatus(TxState.TX_ROLLBACKED_STATE);
                XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
                conn.setXaStatus(TxState.TX_INITIALIZE_STATE);
            } else {
                XAStateLog.saveXARecoveryLog(context.getSessionXaId(), conn);
            }
        }
    }

}
