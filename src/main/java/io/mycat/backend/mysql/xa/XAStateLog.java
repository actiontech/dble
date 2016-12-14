package io.mycat.backend.mysql.xa;

import java.util.Iterator;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.backend.mysql.xa.recovery.impl.FileSystemRepository;
import io.mycat.backend.mysql.xa.recovery.impl.InMemoryRepository;

public class XAStateLog {
	public static final Repository fileRepository = new FileSystemRepository();
	public static final Repository inMemoryRepository = new InMemoryRepository();

	public static void saveXARecoverylog(String xaTXID, TxState sessionState) {
		CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTXID);
		coordinatorLogEntry.setTxState(sessionState);
		flushMemoryRepository(xaTXID, coordinatorLogEntry);
				//will preparing, may success send but failed received,should be rollback
		if (sessionState == TxState.TX_PREPARING_STATE
				//will committing, may success send but failed received,should be commit agagin
				||sessionState == TxState.TX_COMMITING_STATE
				//will rollbacking, may success send but failed received,should be rollback agagin
				||sessionState == TxState.TX_ROLLBACKING_STATE) {
			writeCheckpoint();
		}
	}
	public static void saveXARecoverylog(String xaTXID, MySQLConnection mysqlCon) {
		updateXARecoverylog(xaTXID, mysqlCon, mysqlCon.getXaStatus());
	}

	public static void updateXARecoverylog(String xaTXID, MySQLConnection mysqlCon, TxState txState) {
		updateXARecoverylog(xaTXID, mysqlCon.getHost(), mysqlCon.getPort(), mysqlCon.getSchema(), txState);
	}

	public static void writeCheckpoint() {
		fileRepository.writeCheckpoint(inMemoryRepository.getAllCoordinatorLogEntries());
	}
	public static void updateXARecoverylog(String xaTXID, String host, int port, String schema, TxState txState) {
		CoordinatorLogEntry coordinatorLogEntry = inMemoryRepository.get(xaTXID);
		for (int i = 0; i < coordinatorLogEntry.getParticipants().length; i++) {
			if (coordinatorLogEntry.getParticipants()[i].getSchema().equals(schema)
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
