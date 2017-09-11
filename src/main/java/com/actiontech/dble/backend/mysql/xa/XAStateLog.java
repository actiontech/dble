/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.InMemoryRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class XAStateLog {
    private XAStateLog() {
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(XAStateLog.class);
    private static final Repository FILE_REPOSITORY;

    static {
        if (DbleServer.getInstance().isUseZK()) {
            FILE_REPOSITORY = new KVStoreRepository();
        } else {
            FILE_REPOSITORY = new FileSystemRepository();
        }
    }

    private static final Repository IN_MEMORY_REPOSITORY = new InMemoryRepository();
    private static ReentrantLock lock = new ReentrantLock();
    private static AtomicBoolean hasLeader = new AtomicBoolean(false);
    private static volatile boolean isWriting = false;
    private static Condition waitWriting = lock.newCondition();
    private static ReentrantLock lockNum = new ReentrantLock();
    private static AtomicInteger batchNum = new AtomicInteger(0);
    private static Set<Long> waitSet = new CopyOnWriteArraySet<>();
    private static ConcurrentMap<Long, Boolean> mapResult = new ConcurrentHashMap<>();

    public static boolean saveXARecoverylog(String xaTxId, TxState sessionState) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        coordinatorLogEntry.setTxState(sessionState);
        flushMemoryRepository(xaTxId, coordinatorLogEntry);
        //will preparing, may success send but failed received,should be rollback
        if (sessionState == TxState.TX_PREPARING_STATE ||
                //will committing, may success send but failed received,should be commit agagin
                sessionState == TxState.TX_COMMITING_STATE ||
                //will rollbacking, may success send but failed received,should be rollback agagin
                sessionState == TxState.TX_ROLLBACKING_STATE) {
            return writeCheckpoint(xaTxId);
        }
        return true;
    }

    public static void saveXARecoverylog(String xaTxId, MySQLConnection mysqlCon) {
        updateXARecoverylog(xaTxId, mysqlCon, mysqlCon.getXaStatus());
    }

    private static void updateXARecoverylog(String xaTxId, MySQLConnection mysqlCon, TxState txState) {
        updateXARecoverylog(xaTxId, mysqlCon.getHost(), mysqlCon.getPort(), mysqlCon.getSchema(), txState);
    }

    public static void updateXARecoverylog(String xaTxId, String host, int port, String schema, TxState txState) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        for (int i = 0; i < coordinatorLogEntry.getParticipants().length; i++) {
            if (coordinatorLogEntry.getParticipants()[i] != null &&
                    coordinatorLogEntry.getParticipants()[i].getSchema().equals(schema) &&
                    coordinatorLogEntry.getParticipants()[i].getHost().equals(host) &&
                    coordinatorLogEntry.getParticipants()[i].getPort() == port) {
                coordinatorLogEntry.getParticipants()[i].setTxState(txState);
            }
        }
        flushMemoryRepository(xaTxId, coordinatorLogEntry);
    }

    public static boolean writeCheckpoint(String xaTxId) {
        lock.lock();
        try {
            while (isWriting) {
                waitSet.add(Thread.currentThread().getId());
                waitWriting.await();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("writeCheckpoint error, waiter XID is " + xaTxId, e);
        } finally {
            lock.unlock();
        }
        lockNum.lock();
        try {
            batchNum.incrementAndGet();
            mapResult.put(Thread.currentThread().getId(), false);
        } finally {
            lockNum.unlock();
        }
        if (hasLeader.compareAndSet(false, true)) { // leader thread
            waitSet.remove(Thread.currentThread().getId());
            while (waitSet.size() > 0) {
                //  make all wait thread all became leader or  follower
                Thread.yield();
            }
            lockNum.lock();
            try {
                isWriting = true;
                boolean writeResult = false;
                // copy memoryRepository
                List<CoordinatorLogEntry> logs = new ArrayList<>();
                ReentrantLock lockmap = ((InMemoryRepository) IN_MEMORY_REPOSITORY).getLock();
                lockmap.lock();
                try {
                    Collection<CoordinatorLogEntry> logCollection = IN_MEMORY_REPOSITORY.getAllCoordinatorLogEntries();
                    for (CoordinatorLogEntry coordinatorLogEntry : logCollection) {
                        CoordinatorLogEntry log = coordinatorLogEntry.getDeepCopy();
                        if (log != null) {
                            logs.add(log);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("logCollection deep copy error, leader Xid is:" + xaTxId, e);
                    logs.clear();
                } finally {
                    lockmap.unlock();
                }
                if (!logs.isEmpty()) {
                    writeResult = FILE_REPOSITORY.writeCheckpoint(logs);
                }
                while (batchNum.get() != 1) {
                    Thread.yield();
                }
                batchNum.decrementAndGet();
                hasLeader.set(false);
                lock.lock();
                try {
                    if (writeResult) {
                        for (Long aLong : mapResult.keySet()) {
                            mapResult.put(aLong, true);
                        }
                    }
                    isWriting = false;
                    mapResult.remove(Thread.currentThread().getId());
                    // 1.wakeup follower to return 2.wake up waiting threads continue
                    waitWriting.signalAll();
                    return writeResult;
                } finally {
                    lock.unlock();
                }
            } finally {
                lockNum.unlock();
            }
        } else { // follower thread
            lock.lock();
            try {
                waitSet.remove(Thread.currentThread().getId());
                batchNum.decrementAndGet();
                // the follower's status has copied and ready to write
                waitWriting.await();
                boolean result = mapResult.get(Thread.currentThread().getId());
                mapResult.remove(Thread.currentThread().getId());
                return result;
            } catch (InterruptedException e) {
                LOGGER.warn("writeCheckpoint error, follower Xid is:" + xaTxId, e);
                return false;
            } finally {
                lock.unlock();
            }
        }
    }

    public static void flushMemoryRepository(String xaTxId, CoordinatorLogEntry coordinatorLogEntry) {
        IN_MEMORY_REPOSITORY.put(xaTxId, coordinatorLogEntry);
    }

    public static void initRecoverylog(String xaTxId, int position, MySQLConnection conn) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        coordinatorLogEntry.getParticipants()[position] = new ParticipantLogEntry(xaTxId, conn.getHost(), conn.getPort(), 0,
                conn.getSchema(), conn.getXaStatus());
        flushMemoryRepository(xaTxId, coordinatorLogEntry);
    }

    public static void cleanCompleteRecoverylog() {
        for (CoordinatorLogEntry entry : IN_MEMORY_REPOSITORY.getAllCoordinatorLogEntries()) {
            if (entry.getTxState() == TxState.TX_COMMITED_STATE || entry.getTxState() == TxState.TX_ROLLBACKED_STATE) {
                IN_MEMORY_REPOSITORY.remove(entry.getId());
            }
        }
    }
}
