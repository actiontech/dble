package io.mycat.meta.table;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;

public class SchemaMetaHandler {
	private Lock lock;
	private Condition allSchemaDone;
	private int schemaNumber;

	private MycatConfig config;
	private Set<String> selfNode;

	public SchemaMetaHandler(MycatConfig config, Set<String> selfNode) {
		this.lock = new ReentrantLock();
		this.allSchemaDone = lock.newCondition();
		this.config = config;
		this.selfNode = selfNode;
		schemaNumber = config.getSchemas().size();
	}

	public void execute() {
		for (Entry<String, SchemaConfig> entry : config.getSchemas().entrySet()) {
			MultiTableMetaHandler multiTableMeta = new MultiTableMetaHandler(this, entry.getValue(), selfNode);
			multiTableMeta.execute();
		}
		waitAllNodeDone();
	}

	public void countDown() {
		lock.lock();
		try {
			if (--schemaNumber == 0)
				allSchemaDone.signal();
		} finally {
			lock.unlock();
		}
	}

	public void waitAllNodeDone() {
		lock.lock();
		try {
			if (schemaNumber == 0)
				return;
			allSchemaDone.await();
		} catch (InterruptedException e) {
			// ignore
		} finally {
			lock.unlock();
		}
	}
}
