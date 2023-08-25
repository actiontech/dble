/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.singleton.TransactionCounter;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TransactionService<T extends UserConfig> extends FrontendService<T> {

    private volatile long txId = 0;
    private volatile boolean txStarted = false;
    // private volatile boolean autocommit = false; // in VariablesService

    private AtomicBoolean isLeave = new AtomicBoolean(false);

    public TransactionService(AbstractConnection connection, AuthResultInfo info) {
        super(connection, info);
    }

    public void controlTx(TransactionOperate operate) {
        isLeave.set(false);
        switch (operate) {
            case BEGIN: //  begin、start transaction
                if (!autocommit) {
                    isLeave.set(true);
                } else {
                    txIdCount();
                }
                setTxStart(true);
                break;
            case END: // commit、rollback
            case IMPLICITLY_COMMIT: // DDL. etc(implicitly sql)
                if (!autocommit) {
                    isLeave.set(true);
                } else if (txStarted) {
                    // not deal
                } else {
                    txIdCount();
                }
                setTxStart(false);
                break;
            case AUTOCOMMIT: // = 1/true
                if (!isInTransaction()) {
                    txIdCount();
                } // else not deal

                if (!autocommit) {
                    setTxStart(false);
                }
                setAutocommit(true);
                break;
            case UNAUTOCOMMIT: // = 0/false
                if (!isInTransaction()) {
                    txIdCount();
                } // else not deal

                setAutocommit(false);
                break;
            case QUIT:
                // not deal
                break;
            default: // normal sql
                if (!isInTransaction())
                    txIdCount();
                break;
        }
    }

    public void redressControlTx() {
        if (isLeave.compareAndSet(true, false)) {
            txIdCount();
        }
    }

    public boolean isInTransaction() {
        return (txStarted || !autocommit);
    }

    public void restTxStatus() {
        setTxStart(false);
        setAutocommit(SystemConfig.getInstance().getAutocommit() == 1);
    }

    public long txIdCount() {
        txId = TransactionCounter.txIdGlobalCount();
        return txId;
    }

    // ========================= get/set
    public long getTransactionsCounter() {
        return getTxId();
    }

    public long getTxId() {
        return txId;
    }


    public boolean isTxStart() {
        return txStarted;
    }

    public void setTxStart(boolean txStart) {
        txStarted = txStart;
    }

}
