package com.actiontech.dble.singleton;

import java.util.concurrent.atomic.AtomicLong;

public final class TransactionCounter {
    private static final AtomicLong TX_ID = new AtomicLong(0);

    private TransactionCounter() {
    }

    public static long txIdGlobalCount() {
        return TX_ID.incrementAndGet();
    }

    public static long getCurrentTransactionCount() {
        return TX_ID.get();
    }

}
