/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.table.old.MultiTableMetaHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SchemaMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMetaHandler.class);
    private Lock lock;
    private Condition allSchemaDone;
    private int schemaNumber;

    private ServerConfig config;
    private Set<String> selfNode;
    private final ProxyMetaManager tmManager;

    public SchemaMetaHandler(ProxyMetaManager tmManager, ServerConfig config, Set<String> selfNode) {
        this.tmManager = tmManager;
        this.lock = new ReentrantLock();
        this.allSchemaDone = lock.newCondition();
        this.config = config;
        this.selfNode = selfNode;
        schemaNumber = config.getSchemas().size();
    }

    public void execute() {
        for (Entry<String, SchemaConfig> entry : config.getSchemas().entrySet()) {
            if (DbleServer.getInstance().getConfig().getSystem().getUseOldMetaInit() == 1) {
                MultiTableMetaHandler multiTableMeta = new MultiTableMetaHandler(this, entry.getValue(), selfNode);
                multiTableMeta.execute();
            } else {
                MultiTablesMetaHandler multiTableMeta = new MultiTablesMetaHandler(this, entry.getValue(), selfNode);
                multiTableMeta.execute();
            }
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
            while (schemaNumber != 0) {
                allSchemaDone.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("waitAllNodeDone " + e);
        } finally {
            lock.unlock();
        }
    }

    public ProxyMetaManager getTmManager() {
        return tmManager;
    }
}
