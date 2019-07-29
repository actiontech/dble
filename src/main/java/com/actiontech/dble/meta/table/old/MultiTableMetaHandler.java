/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table.old;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.table.SchemaMetaHandler;
import com.actiontech.dble.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiTableMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTableMetaHandler.class);
    private AtomicInteger shardTableCnt;
    private AtomicInteger singleTableCnt;
    private AtomicBoolean countDownFlag = new AtomicBoolean(false);
    private String schema;
    private SchemaConfig config;
    private SchemaMetaHandler schemaMetaHandler;
    private Set<String> selfNode;
    private Lock singleTableLock = new ReentrantLock();
    private Condition collectTables = singleTableLock.newCondition();
    private Set<String> filterTables;

    public MultiTableMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig config, Set<String> selfNode) {
        this.schemaMetaHandler = schemaMetaHandler;
        this.config = config;
        this.schema = config.getName();
        this.selfNode = selfNode;
        this.shardTableCnt = new AtomicInteger(config.getTables().size());
        this.singleTableCnt = new AtomicInteger(0);
    }

    public void execute() {
        this.schemaMetaHandler.getTmManager().createDatabase(schema);
        boolean existTable = false;
        if (config.getDataNode() != null && (selfNode == null || !selfNode.contains(config.getDataNode()))) {
            List<String> tables = getSingleTables();
            if (!CollectionUtil.isEmpty(filterTables)) {
                tables.retainAll(filterTables);
                filterTables.removeAll(tables);
            }
            singleTableCnt.set(tables.size());
            for (String table : tables) {
                existTable = true;
                AbstractTableMetaHandler tableHandler = new SingleTableMetaInitHandler(this, schema, table, Collections.singletonList(config.getDataNode()), selfNode);
                tableHandler.execute();
            }
        }
        for (Entry<String, TableConfig> entry : filterConfigTables().entrySet()) {
            existTable = true;
            AbstractTableMetaHandler tableHandler = new TableMetaInitHandler(this, schema, entry.getValue(), selfNode);
            tableHandler.execute();
        }
        if (!existTable) {
            countDown();
        }
    }

    private Map<String, TableConfig> filterConfigTables() {
        Map<String, TableConfig> newReload = new HashMap<>();
        if (filterTables == null) {
            newReload = config.getTables();
        } else {
            for (String table : filterTables) {
                if (config.getTables().containsKey(table)) {
                    newReload.put(table, config.getTables().get(table));
                } else {
                    LOGGER.warn("reload table[" + schema + "." + table + "] metadata, but table doesn't exist");
                }
            }
        }
        return newReload;
    }

    private List<String> getSingleTables() {
        SchemaDefaultNodeTablesHandler showTablesHandler = new SchemaDefaultNodeTablesHandler(this, config);
        showTablesHandler.execute();
        singleTableLock.lock();
        try {
            while (!showTablesHandler.isFinished()) {
                collectTables.await();
            }
        } catch (InterruptedException e) {
            LOGGER.info("getSingleTables " + e);
            return new ArrayList<>();
        } finally {
            singleTableLock.unlock();
        }
        return showTablesHandler.getTables();
    }

    void showTablesFinished() {
        singleTableLock.lock();
        try {
            collectTables.signal();
        } finally {
            singleTableLock.unlock();
        }
    }

    void countDownSingleTable() {
        if (singleTableCnt.decrementAndGet() == 0) {
            countDown();
        }
    }

    void countDownShardTable() {
        if (shardTableCnt.decrementAndGet() == 0) {
            countDown();
        }
    }

    private void countDown() {
        if (shardTableCnt.get() == 0 && singleTableCnt.get() == 0) {
            if (countDownFlag.compareAndSet(false, true)) {
                schemaMetaHandler.countDown();
            }
        }
    }

    public ProxyMetaManager getTmManager() {
        return this.schemaMetaHandler.getTmManager();
    }

    public void setFilterTables(Set<String> filterTables) {
        this.filterTables = filterTables;
    }
}
