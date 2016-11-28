package io.mycat.meta.table;

import java.util.Map.Entry;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;

public class SchemaMetaHandler {

	// 1、根据用户、DataSourceConfig获取所有的节点数据源。
	//  2、show databases 获取当前节点的schema。
	// 3、如果是分库schema，只有节点0需要获取table信息。
	// 4、一次获取每个schema下的表信息、索引信息。（如配置了二级拆分规则，则该表忽略）

	private Lock lock;
	private Condition allSchemaDone;
	private int schemaNumber;

	private MycatConfig config;

	public SchemaMetaHandler(MycatConfig config) {
		this.lock = new ReentrantLock();
		this.allSchemaDone = lock.newCondition();
		this.config = config;
		schemaNumber = config.getSchemas().size();
	}

	public void execute() {
		for (Entry<String, SchemaConfig> entry : config.getSchemas().entrySet()) {
			MultiTableMetaHandler multiTableMeta = new MultiTableMetaHandler(this,  entry.getValue());
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
