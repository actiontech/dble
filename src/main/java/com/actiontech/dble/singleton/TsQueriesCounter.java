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
    private long hisTransCount;

    private TsQueriesCounter() {

    }

    public CalculateResult calculate() {
        long queries = 0;
        long transactions = 0;
        lock.readLock().lock();
        try {
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (FrontendConnection fc : processor.getFrontends().values()) {
                    if (!fc.isManager()) {
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
