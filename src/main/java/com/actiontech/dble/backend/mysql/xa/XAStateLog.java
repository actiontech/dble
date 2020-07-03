/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;


import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.InMemoryRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
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
        if (ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode()) {
            FILE_REPOSITORY = new KVStoreRepository();
        } else {
            FILE_REPOSITORY = new FileSystemRepository();
        }
    }

    public static final String XA_ALERT_FLAG = "XA_ALERT_FLAG";
    private static final Repository IN_MEMORY_REPOSITORY = new InMemoryRepository();
    private static ReentrantLock lock = new ReentrantLock();
    private static AtomicBoolean hasLeader = new AtomicBoolean(false);
    private static volatile boolean isWriting = false;
    private static Condition waitWriting = lock.newCondition();
    private static ReentrantLock lockNum = new ReentrantLock();
    private static AtomicInteger batchNum = new AtomicInteger(0);
    private static Set<Long> waitSet = new CopyOnWriteArraySet<>();
    private static ConcurrentMap<Long, Boolean> mapResult = new ConcurrentHashMap<>();
    private static volatile boolean writeAlert = false;

    public static boolean saveXARecoveryLog(String xaTxId, TxState sessionState) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        coordinatorLogEntry.setTxState(sessionState);
        flushMemoryRepository(xaTxId, coordinatorLogEntry);
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            return true;
        }
        //will preparing, may success send but failed received,should be rollback
        if (sessionState == TxState.TX_PREPARING_STATE ||
                //will committing, may success send but failed received,should be commit agagin
                sessionState == TxState.TX_COMMITTING_STATE ||
                //will rollbacking, may success send but failed received,should be rollback agagin
                sessionState == TxState.TX_ROLLBACKING_STATE) {
            return writeCheckpoint(xaTxId);
        }
        return true;
    }

    public static void saveXARecoveryLog(String xaTxId, MySQLResponseService service) {
        updateXARecoveryLog(xaTxId, service, service.getXaStatus());
    }

    private static void updateXARecoveryLog(String xaTxId, MySQLResponseService service, TxState txState) {
        long expires = ((RouteResultsetNode) service.getAttachment()).getMultiplexNum().longValue();
        updateXARecoveryLog(xaTxId, service.getConnection().getHost(), service.getConnection().getPort(), service.getConnection().getSchema(), expires, txState);
    }

    public static void updateXARecoveryLog(String xaTxId, String host, int port, String schema, long expires, TxState txState) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        for (int i = 0; i < coordinatorLogEntry.getParticipants().length; i++) {
            if (coordinatorLogEntry.getParticipants()[i] != null &&
                    coordinatorLogEntry.getParticipants()[i].getSchema().equals(schema) &&
                    coordinatorLogEntry.getParticipants()[i].getHost().equals(host) &&
                    coordinatorLogEntry.getParticipants()[i].getPort() == port &&
                    coordinatorLogEntry.getParticipants()[i].getExpires() == expires) {
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
                ReentrantLock lockMap = ((InMemoryRepository) IN_MEMORY_REPOSITORY).getLock();
                lockMap.lock();
                try {
                    Collection<CoordinatorLogEntry> logCollection = IN_MEMORY_REPOSITORY.getAllCoordinatorLogEntries(false);
                    for (CoordinatorLogEntry coordinatorLogEntry : logCollection) {
                        CoordinatorLogEntry log = coordinatorLogEntry.getDeepCopy();
                        if (log != null) {
                            logs.add(log);
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.warn("logCollection deep copy error, leader Xid is:" + xaTxId, e);
                    logs.clear();
                } finally {
                    lockMap.unlock();
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
                // the follower's status has copied and ready to writeDirectly
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

    public static void initRecoveryLog(String xaTxId, int position, MySQLResponseService service) {
        CoordinatorLogEntry coordinatorLogEntry = IN_MEMORY_REPOSITORY.get(xaTxId);
        long expires = ((RouteResultsetNode) service.getAttachment()).getMultiplexNum().longValue();
        coordinatorLogEntry.getParticipants()[position] = new ParticipantLogEntry(xaTxId, service.getConnection().getHost(), service.getConnection().getPort(), expires,
                service.getSchema(), service.getXaStatus());
        flushMemoryRepository(xaTxId, coordinatorLogEntry);
    }

    public static void cleanCompleteRecoveryLog() {
        for (CoordinatorLogEntry entry : IN_MEMORY_REPOSITORY.getAllCoordinatorLogEntries(false)) {
            if (entry.getTxState() == TxState.TX_COMMITTED_STATE || entry.getTxState() == TxState.TX_ROLLBACKED_STATE) {
                IN_MEMORY_REPOSITORY.remove(entry.getId());
            }
        }
    }

    public static void setWriteAlert(boolean writeAlert) {
        XAStateLog.writeAlert = writeAlert;
    }

    public static boolean isWriteAlert() {
        return writeAlert;
    }

}
