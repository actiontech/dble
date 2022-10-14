package com.actiontech.dble.services;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.AuthResultInfo;

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

                setTxStart(false);
                setAutocommit(true);
                break;
            case UNAUTOCOMMIT: // = 0/false
                if (!isInTransaction()) {
                    txIdCount();
                } // else not deal

                setAutocommit(false);
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
        setAutocommit(SystemConfig.getInstance().getAutocommit() == 0);
    }

    public long txIdCount() {
        return txId.incrementAndGet();
    }

    public void resetTxId() {
        txId.set(Long.MIN_VALUE);
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
