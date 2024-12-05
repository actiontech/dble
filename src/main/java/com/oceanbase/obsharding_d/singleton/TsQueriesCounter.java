/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.services.BusinessService;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by szf on 2019/12/16.
 */
public final class TsQueriesCounter {
    private static final TsQueriesCounter INSTANCE = new TsQueriesCounter();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long hisQueriesCount;
    private long hisTransCount;

    private TsQueriesCounter() {

    }

    public CalculateResult calculate() {
        long queries = 0;
        long transactions = 0;
        lock.readLock().lock();
        try {
            for (IOProcessor processor : OBsharding_DServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fc : processor.getFrontends().values()) {
                    if (!fc.isManager() && fc.getService() instanceof BusinessService) {
                        long query = ((BusinessService) (fc.getService())).getQueriesCounter();
                        long transaction = ((BusinessService) (fc.getService())).getTransactionsCounter();
                        queries += query > 0 ? query : 0;
                        transactions += transaction > 0 ? transaction : transaction;
                    }
                }
            }
            queries += hisQueriesCount;
            transactions += hisTransCount;
        } finally {
            lock.readLock().unlock();
        }
        return new CalculateResult(queries, transactions);
    }

    public void addToHistory(BusinessService service) {
        lock.writeLock().lock();
        try {
            if (service.getQueriesCounter() > 0) {
                hisQueriesCount += service.getQueriesCounter();
                hisTransCount += service.getTransactionsCounter();
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
