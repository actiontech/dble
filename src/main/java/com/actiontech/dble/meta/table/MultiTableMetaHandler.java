/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiTableMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTableMetaHandler.class);
    private AtomicInteger shardTableCnt;
    private AtomicInteger singleTableCnt;
    private String schema;
    private SchemaConfig config;
    private SchemaMetaHandler schemaMetaHandler;
    private Set<String> selfNode;
    private Lock singleTableLock = new ReentrantLock();
    private Condition collectTables = singleTableLock.newCondition();

    MultiTableMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig config, Set<String> selfNode) {
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
        if (config.getDataNode() != null) {
            List<String> tables = getSingleTables();
            singleTableCnt.set(tables.size());
            for (String table : tables) {
                existTable = true;
                AbstractTableMetaHandler tableHandler = new SingleTableMetaInitHandler(this, schema, table, Collections.singletonList(config.getDataNode()), selfNode);
                tableHandler.execute();
            }
        }
        for (Entry<String, TableConfig> entry : config.getTables().entrySet()) {
            existTable = true;
            AbstractTableMetaHandler tableHandler = new TableMetaInitHandler(this, schema, entry.getValue(), selfNode);
            tableHandler.execute();
        }
        if (!existTable) {
            countDown();
        }
    }

    private List<String> getSingleTables() {
        ShowTablesHandler showTablesHandler = new ShowTablesHandler(this, config);
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
            schemaMetaHandler.countDown();
        }
    }

    public ProxyMetaManager getTmManager() {
        return this.schemaMetaHandler.getTmManager();
    }
}
