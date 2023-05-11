/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.services.BusinessService;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by szf on 2019/12/16.
 */
public final class TsQueriesCounter {
    private static final TsQueriesCounter INSTANCE = new TsQueriesCounter();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long hisQueriesCount;

    private TsQueriesCounter() {

    }

    public CalculateResult calculate() {
        long queries = 0;
        lock.readLock().lock();
        try {
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fc : processor.getFrontends().values()) {
                    if (!fc.isManager() && fc.getService() instanceof BusinessService) {
                        long query = ((BusinessService) (fc.getService())).getQueriesCounter();
                        queries += query > 0 ? query : 0;
                    }
                }
            }
            queries += hisQueriesCount;
        } finally {
            lock.readLock().unlock();
        }
        return new CalculateResult(queries, TransactionCounter.getCurrentTransactionCount());
    }

    public void addToHistory(BusinessService service) {
        lock.writeLock().lock();
        try {
            if (service.getQueriesCounter() > 0) {
                hisQueriesCount += service.getQueriesCounter();
                service.resetCounter();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    public static TsQueriesCounter getInstance() {
        return INSTANCE;
    }

    public static class CalculateResult {
        public final long queries;
        public final long transactions;

        CalculateResult(long queries, long transactions) {
            this.queries = queries;
            this.transactions = transactions;
        }
    }
}
