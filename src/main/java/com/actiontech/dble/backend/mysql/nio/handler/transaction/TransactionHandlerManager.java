package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.handler.NormalTransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.XAHandler;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionHandlerManager {

    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionHandlerManager.class);

    private volatile boolean retryXa = true;
    private volatile String xaTxId;
    private TransactionHandler normalHandler;
    private TransactionHandler xaHandler;

    public TransactionHandlerManager(NonBlockingSession session) {
        this.normalHandler = new NormalTransactionHandler(session);
        this.xaHandler = new XAHandler(session);
    }

    public String getSessionXaID() {
        return xaTxId;
    }

    public void setXaTxEnabled(boolean xaTXEnabled, ShardingService source) {
        if (xaTXEnabled && this.xaTxId == null) {
            LOGGER.info("XA Transaction enabled ,con " + source);
            xaTxId = DbleServer.getInstance().genXaTxId();
        } else if (!xaTXEnabled && this.xaTxId != null) {
            LOGGER.info("XA Transaction disabled ,con " + source);
            xaTxId = null;
        }
    }

    public void setRetryXa(boolean retry) {
        this.retryXa = retry;
    }

    public boolean isRetryXa() {
        return retryXa;
    }

    public String getXAStage() {
        if (xaTxId != null) {
            return ((AbstractXAHandler) xaHandler).getXAStage();
        }
        return null;
    }

    public TransactionHandler getTransactionHandler() {
        if (xaTxId != null) {
            return xaHandler;
        } else {
            return normalHandler;
        }
    }

    public void commit() {
        if (xaTxId != null) {
            xaHandler.commit();
        } else {
            normalHandler.commit();
        }
    }

    public void implicitCommit(ImplicitCommitHandler handler) {
        if (xaTxId != null) {
            xaHandler.implicitCommit(handler);
        } else {
            normalHandler.implicitCommit(handler);
        }
    }

    public void rollback() {
        if (xaTxId != null) {
            xaHandler.rollback();
        } else {
            normalHandler.rollback();
        }
    }

}
