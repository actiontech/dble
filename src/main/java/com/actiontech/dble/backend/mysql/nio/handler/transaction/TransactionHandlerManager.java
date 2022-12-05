/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.handler.NormalTransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler.XAHandler;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class TransactionHandlerManager {

    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionHandlerManager.class);

    private volatile boolean retryXa = true;
    private volatile String xaTxId;
    private TransactionHandler normalHandler;
    private TransactionHandler xaHandler;
    private NonBlockingSession session;

    public TransactionHandlerManager(NonBlockingSession session) {
        this.normalHandler = new NormalTransactionHandler(session);
        this.xaHandler = new XAHandler(session);
        this.session = session;
    }

    public String getSessionXaID() {
        return xaTxId;
    }

    public boolean isXaEnabled() {
        return xaTxId != null;
    }

    public void setXaTxEnabled(boolean xaTXEnabled, ShardingService service) {
        if (xaTXEnabled && this.xaTxId == null) {
            LOGGER.info("XA Transaction enabled ,con " + service.getConnection());
            xaTxId = DbleServer.getInstance().genXaTxId();
            StatisticListener.getInstance().record(service, r -> r.onXaStart(xaTxId));
        } else if (!xaTXEnabled && this.xaTxId != null) {
            LOGGER.info("XA Transaction disabled ,con " + service.getConnection());
            StatisticListener.getInstance().record(service, r -> r.onXaStop());
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

    public void commit(TransactionCallback callback) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} execute commit(), current {}", session.getShardingService().toString2(), session);
        }
        if (xaTxId != null) {
            xaHandler.commit(callback);
        } else {
            normalHandler.commit(callback);
        }
    }

    public void syncImplicitCommit() throws SQLException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} execute syncImplicitCommit(), current {}", session.getShardingService().toString2(), session);
        }
        if (xaTxId != null) {
            // implicit commit is not supported in XA transactions
            // xaHandler.syncImplicitCommit();
        } else {
            normalHandler.syncImplicitCommit();
        }
    }

    public void rollback(TransactionCallback callback) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} execute rollback(), current {}", session.getShardingService().toString2(), session);
        }
        if (xaTxId != null) {
            xaHandler.rollback(callback);
        } else {
            normalHandler.rollback(callback);
        }
    }

}
