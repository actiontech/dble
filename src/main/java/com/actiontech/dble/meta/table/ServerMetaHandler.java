/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.ReloadLogHelper;
import com.actiontech.dble.meta.ReloadManager;
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

/**
 * Start handler for the entire meta init/reload event
 */
public class ServerMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetaHandler.class);
    private final ProxyMetaManager tmManager;
    private Lock lock;
    private Condition allSchemaDone;
    private int schemaNumber;
    private ServerConfig config;
    private Set<String> selfNode;
    private Map<String, Set<String>> filter;
    private Map<String, SchemaConfig> reloadSchemas;

    public ServerMetaHandler(ProxyMetaManager tmManager, ServerConfig config, Set<String> selfNode) {
        this.tmManager = tmManager;
        this.lock = new ReentrantLock();
        this.allSchemaDone = lock.newCondition();
        this.config = config;
        this.selfNode = selfNode;
        this.reloadSchemas = config.getSchemas();
        this.schemaNumber = config.getSchemas().size();
    }

    private void filter() {
        if (CollectionUtil.isEmpty(filter)) {
            return;
        }

        Map<String, SchemaConfig> newReload = new HashMap<>();
        for (Entry<String, Set<String>> entry : filter.entrySet()) {
            String schema = entry.getKey();
            if (config.getSchemas().containsKey(schema)) {
                newReload.put(schema, config.getSchemas().get(schema));
            } else {
                ReloadLogHelper.warn("reload schema[" + schema + "] metadata, but schema doesn't exist", LOGGER);
            }
        }
        this.reloadSchemas = newReload;
        this.schemaNumber = reloadSchemas.size();
    }

    public boolean execute() {
        filter();
        ReloadLogHelper.infoList("Meta reload ", LOGGER, reloadSchemas.keySet());
        for (Entry<String, SchemaConfig> entry : reloadSchemas.entrySet()) {
            if (ReloadManager.getReloadInstance().isReloadInterrupted()) {
                ReloadLogHelper.info("reload meta loop interrupted by command ,break the loop", LOGGER);
                break;
            }
            SchemaInitMetaHandler multiTableMeta = new SchemaInitMetaHandler(this, entry.getValue(), selfNode);
            if (filter != null) {
                multiTableMeta.setFilterTables(filter.get(entry.getKey()));
                ReloadLogHelper.infoList("sharding filter " + entry.getKey(), LOGGER, filter.get(entry.getKey()));
            }
            multiTableMeta.execute();
        }
        return waitAllSchemaDone();
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

    /**
     * waitAllSchemaNode meta init done,true means stop with normal ,false means stop with Interrupted
     *
     * @return
     */
    public boolean waitAllSchemaDone() {
        lock.lock();
        try {
            while (schemaNumber != 0 && !ReloadManager.getReloadInstance().isReloadInterrupted()) {
                allSchemaDone.await();
            }
        } catch (InterruptedException e) {
            ReloadLogHelper.info("waitAllSchemaDone " + e, LOGGER);
        } finally {
            lock.unlock();
        }
        return !ReloadManager.getReloadInstance().isReloadInterrupted();
    }

    public ProxyMetaManager getTmManager() {
        return tmManager;
    }

    public void register() {
        ReloadManager.getReloadInstance().getStatus().register(this);
    }

    public void release() {
        lock.lock();
        try {
            allSchemaDone.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void setFilter(Map<String, Set<String>> filter) {
        this.filter = filter;
    }
}
