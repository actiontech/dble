package io.mycat.backend.mysql.xa.recovery.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.recovery.Repository;

/**
 * Created by zhangchao on 2016/10/18.
 */
public class InMemoryRepository implements Repository {

    private Map<String, CoordinatorLogEntry> storage = new ConcurrentHashMap<String, CoordinatorLogEntry>();


    private boolean closed = true;
    @Override
    public void init() {
        closed=false;
    }

    @Override
    public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
        storage.put(id, coordinatorLogEntry);
    }

    @Override
    public CoordinatorLogEntry get(String coordinatorId) {
        return storage.get(coordinatorId);
    }

    @Override
    public void close() {
        storage.clear();
        closed=true;
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        return storage.values();
    }

    @Override
    public void writeCheckpoint(
            Collection<CoordinatorLogEntry> checkpointContent) {
        storage.clear();
        for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
            storage.put(coordinatorLogEntry.getId(), coordinatorLogEntry);
        }

    }



    public boolean isClosed() {
        return closed;
    }

	@Override
	public synchronized void remove(String id) {
		if(storage.get(id).getTxState()==TxState.TX_COMMITED_STATE ||storage.get(id).getTxState()==TxState.TX_ROLLBACKED_STATE){
			storage.remove(id);
		}
	}
}
