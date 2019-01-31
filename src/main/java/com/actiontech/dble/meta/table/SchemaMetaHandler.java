/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.table.old.MultiTableMetaHandler;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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
    private Map<String, Set<String>> filter;
    private Map<String, SchemaConfig> reloadSchemas;

    public SchemaMetaHandler(ProxyMetaManager tmManager, ServerConfig config, Set<String> selfNode) {
        this.tmManager = tmManager;
        this.lock = new ReentrantLock();
        this.allSchemaDone = lock.newCondition();
        this.config = config;
        this.selfNode = selfNode;
        this.reloadSchemas = config.getSchemas();
        this.schemaNumber = config.getSchemas().size();
    }

    private void filter() {
        if (!CollectionUtil.isEmpty(filter)) {
            Map<String, SchemaConfig> newReload = new HashMap<>();
            for (Entry<String, Set<String>> entry : filter.entrySet()) {
                String schema = entry.getKey();
                if (config.getSchemas().containsKey(schema)) {
                    newReload.put(schema, config.getSchemas().get(schema));
                } else {
                    LOGGER.warn("reload schema[" + schema + "] metadata, but schema doesn't exist");
                }
            }
            this.reloadSchemas = newReload;
            this.schemaNumber = reloadSchemas.size();
        }
    }

    public void execute() {
        filter();
        for (Entry<String, SchemaConfig> entry : reloadSchemas.entrySet()) {
            if (DbleServer.getInstance().getConfig().getSystem().getUseOldMetaInit() == 1) {
                MultiTableMetaHandler multiTableMeta = new MultiTableMetaHandler(this, entry.getValue(), selfNode);
                if (filter != null) {
                    multiTableMeta.setFilterTables(filter.get(entry.getKey()));
                }
                multiTableMeta.execute();
            } else {
                MultiTablesMetaHandler multiTableMeta = new MultiTablesMetaHandler(this, entry.getValue(), selfNode);
                if (filter != null) {
                    multiTableMeta.setFilterTables(filter.get(entry.getKey()));
                }
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

    public void setFilter(Map<String, Set<String>> filter) {
        this.filter = filter;
    }
}
