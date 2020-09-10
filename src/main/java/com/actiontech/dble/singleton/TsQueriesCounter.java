package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

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
                    if (!fc.isManager() && fc.getFrontEndService() instanceof ShardingService) {
                        long query = ((ShardingService) fc.getFrontEndService()).getSession2().getQueriesCounter();
                        long transaction = ((ShardingService) fc.getFrontEndService()).getSession2().getTransactionsCounter();
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

    public void addToHistory(NonBlockingSession session) {
        lock.writeLock().lock();
        try {
            if (session.getQueriesCounter() > 0) {
                hisQueriesCount += session.getQueriesCounter();
                hisTransCount += session.getTransactionsCounter();
                session.resetCounter();
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
