/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class TransactionService<T extends UserConfig> extends FrontendService<T> {

    private final AtomicLong txId = new AtomicLong(0);
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
        return txId.incrementAndGet();
    }

    public void resetTxId() {
        txId.set(0);
    }

    // ========================= get/set
    public long getTransactionsCounter() {
        return getTxId();
    }

    public long getTxId() {
        return txId.get();
    }

    public boolean isTxStart() {
        return txStarted;
    }

    public void setTxStart(boolean txStart) {
        txStarted = txStart;
    }

}
