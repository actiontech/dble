package io.mycat.backend.mysql.xa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.backend.mysql.xa.recovery.impl.FileSystemRepository;
import io.mycat.backend.mysql.xa.recovery.impl.InMemoryRepository;

public class XAStateLog {
	public static final Logger logger = LoggerFactory.getLogger(XAStateLog.class);
	public static final Repository fileRepository = new FileSystemRepository();
	public static final Repository inMemoryRepository = new InMemoryRepository();
	private static ReentrantLock lock = new ReentrantLock();
	private static AtomicBoolean hasLeader = new AtomicBoolean(false);
	private static volatile boolean isWriting = false;
	private static Condition waitWriting = lock.newCondition();
	private static volatile boolean writeResult = false;
	private static ReentrantLock lockNum = new ReentrantLock();
	private static AtomicInteger batchNum = new AtomicInteger(0);
	private static Set<Long> waitSet = new CopyOnWriteArraySet<Long>();
	public static boolean saveXARecoverylog(String xaTXID, TxState sessionState) {
		CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTXID);
		coordinatorLogEntry.setTxState(sessionState);
		flushMemoryRepository(xaTXID, coordinatorLogEntry);
				//will preparing, may success send but failed received,should be rollback
		if (sessionState == TxState.TX_PREPARING_STATE
				//will committing, may success send but failed received,should be commit agagin
				||sessionState == TxState.TX_COMMITING_STATE
				//will rollbacking, may success send but failed received,should be rollback agagin
				||sessionState == TxState.TX_ROLLBACKING_STATE) {
			return writeCheckpoint(xaTXID);
		}
		return true;
	}
	public static void saveXARecoverylog(String xaTXID, MySQLConnection mysqlCon) {
		updateXARecoverylog(xaTXID, mysqlCon, mysqlCon.getXaStatus());
	}

	public static void updateXARecoverylog(String xaTXID, MySQLConnection mysqlCon, TxState txState) {
		updateXARecoverylog(xaTXID, mysqlCon.getHost(), mysqlCon.getPort(), mysqlCon.getSchema(), txState);
	}

	public static boolean writeCheckpoint(String xaTXID) {
		lock.lock();
		try {
			if (isWriting) {
				waitSet.add(Thread.currentThread().getId());
				waitWriting.await();
			}
		} catch (InterruptedException e) {
			logger.warn("writeCheckpoint error, waiter XID is " + xaTXID , e);
		} finally {
			lock.unlock();
		}
		lockNum.lock();
		try {
			batchNum.incrementAndGet();
		} finally {
			lockNum.unlock();
		}
		if (hasLeader.compareAndSet(false, true)) {// leader thread
			waitSet.remove(Thread.currentThread().getId());
			while (waitSet.size() > 0) {
				//  make all wait thread all became leader or  follower
				Thread.yield();
			}
			lockNum.lock();
			try {
				isWriting = true;
				writeResult = false;
				// copy memoryRepository
				List<CoordinatorLogEntry> logs = new ArrayList<>();
				ReentrantLock lockmap = ((InMemoryRepository) inMemoryRepository).getLock();
				lockmap.lock();
				try {
					Collection<CoordinatorLogEntry> logCollection = inMemoryRepository.getAllCoordinatorLogEntries();
					for (CoordinatorLogEntry coordinatorLogEntry : logCollection) {
						CoordinatorLogEntry log = coordinatorLogEntry.getDeepCopy();
						if (log != null) {
							logs.add(log);
						}
					}
				} catch (Exception e) {
					logger.warn("logCollection deep copy error, leader Xid is:" + xaTXID, e);
					logs.clear();
				} finally {
					lockmap.unlock();
				}
				if (!logs.isEmpty()) {
					writeResult = fileRepository.writeCheckpoint(logs);
				}
				while (batchNum.get() != 1) {
					Thread.yield();
				}
				batchNum.decrementAndGet();
				hasLeader.set(false);
				lock.lock();
				try {
					isWriting = false;
					// 1.wakeup follower to return 2.wake up waiting threads continue
					waitWriting.signalAll();
					return writeResult;
				} finally {
					lock.unlock();
				}
			} finally {
				lockNum.unlock();
			}
		} else {// follower thread
			lock.lock();
			try {
				waitSet.remove(Thread.currentThread().getId());
				batchNum.decrementAndGet();
				// the follower's status has copied and ready to write
				waitWriting.await();
				return writeResult;
			} catch (InterruptedException e) {
				logger.warn("writeCheckpoint error, follower Xid is:" + xaTXID, e);
				return false;
			} finally {
				lock.unlock();
			}
		}
	}
	public static void updateXARecoverylog(String xaTXID, String host, int port, String schema, TxState txState) {
		CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTXID);
		for (int i = 0; i < coordinatorLogEntry.getParticipants().length; i++) {
			if (coordinatorLogEntry.getParticipants()[i] != null
					&& coordinatorLogEntry.getParticipants()[i].getSchema().equals(schema)
					&& coordinatorLogEntry.getParticipants()[i].getHost().equals(host)
					&& coordinatorLogEntry.getParticipants()[i].getPort() == port) {
				coordinatorLogEntry.getParticipants()[i].setTxState(txState);
			}
		}
		flushMemoryRepository(xaTXID, coordinatorLogEntry);
	}
	public static void flushMemoryRepository(String xaTXID, CoordinatorLogEntry coordinatorLogEntry){
		inMemoryRepository.put(xaTXID, coordinatorLogEntry);
	}

	public static void initRecoverylog(String xaTXID, int position, MySQLConnection conn) {
		CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTXID);
		coordinatorLogEntry.getParticipants()[position] = new ParticipantLogEntry(xaTXID, conn.getHost(), conn.getPort(), 0,
				conn.getSchema(), conn.getXaStatus());
		flushMemoryRepository(xaTXID, coordinatorLogEntry);
	}

	public static void cleanCompleteRecoverylog() {
		Iterator<CoordinatorLogEntry> coordinatorLogIterator = inMemoryRepository.getAllCoordinatorLogEntries().iterator();
		while (coordinatorLogIterator.hasNext()) {
			CoordinatorLogEntry entry = coordinatorLogIterator.next();
			if (entry.getTxState() != TxState.TX_COMMITED_STATE && entry.getTxState() != TxState.TX_ROLLBACKED_STATE) {
				continue;
			} else {
				inMemoryRepository.remove(entry.getId());
			}
		}
	}
}
